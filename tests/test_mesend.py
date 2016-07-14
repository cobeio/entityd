import argparse
import pathlib
import re
import time

import act
import cobe
import msgpack
import pytest
import zmq
import zmq.auth
import zmq.auth.thread

import entityd
import entityd.mesend


def get_receiver(endpoint, request, keydir):
    # Create a receiver socket, intiailize it with default options and
    # authentication if a key directory has been provided. Install finalizers
    # to clean everything up at the end then wait for the socket to be fully
    # bound before returning it.
    context = zmq.Context()
    sock = context.socket(zmq.PULL)
    sock.LINGER = 300
    if keydir:
        server_public, server_secret = zmq.auth.load_certificate(
            keydir.join('modeld.key_secret').strpath)
        auth = zmq.auth.thread.ThreadAuthenticator(context)
        auth.start()
        auth.configure_curve(domain='*', location=keydir.strpath)
        sock.CURVE_PUBLICKEY = server_public
        sock.CURVE_SECRETKEY = server_secret
        sock.CURVE_SERVER = True

    # Must call sock.close first, else context.term will block.
    def term():
        if keydir:
            auth.stop()
        sock.close(linger=0)
        context.term()
    request.addfinalizer(term)
    sock.bind(endpoint)
    for _ in range(100):
        if sock.LAST_ENDPOINT != b'':
            break
        time.sleep(0.01)
    else:
        assert False, 'Socket is not bound'
    return sock


@pytest.fixture
def receiver(request, certificates):
    keydir = certificates.join('modeld', 'keys')
    return get_receiver('tcp://*:*', request, keydir)


def get_sender(endpoint, keydir):
    session = pytest.Mock()
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    session.config.args.dest = endpoint
    session.config.keydir = pathlib.Path(str(keydir))
    return sender


@pytest.fixture
def sender(certificates):
    keydir = certificates.join('entityd', 'keys')
    return get_sender('tcp://127.0.0.1:25010', keydir)


@pytest.fixture
def sender_receiver(certificates, receiver):
    """Get an ME Sender with a matched receiving socket with random port."""
    keydir = certificates.join('entityd', 'keys')
    return get_sender(receiver.LAST_ENDPOINT, keydir), receiver


def test_option_default():
    parser = argparse.ArgumentParser()
    entityd.mesend.MonitoredEntitySender().entityd_addoption(parser)
    args = parser.parse_args([])
    assert args.dest == 'tcp://127.0.0.1:25010'


def test_addoption():
    parser = argparse.ArgumentParser()
    entityd.mesend.MonitoredEntitySender().entityd_addoption(parser)
    args = parser.parse_args(['--dest', 'tcp://192.168.0.1:7890'])
    assert args.dest == 'tcp://192.168.0.1:7890'


def test_configure(sender, config):
    sender.entityd_configure(config)
    assert config.keydir == act.fsloc.sysconfdir.joinpath('entityd', 'keys')


def test_sessionstart():
    session = pytest.Mock()
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    assert sender.session == session
    assert isinstance(sender.context, zmq.Context)


def test_sessionfinish():
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(pytest.Mock())
    sender._socket = pytest.Mock()
    context = sender.context
    sender.entityd_sessionfinish()
    sender.socket.close.assert_called_once_with(linger=500)
    assert context.closed


@pytest.mark.parametrize('deleted', [True, False])
def test_send_entity(sender_receiver, deleted):
    sender, receiver = sender_receiver
    entity = entityd.EntityUpdate('MeType')
    entity.label = 'entity label'
    if deleted:
        entity.delete()
    sender.entityd_send_entity(entity)
    assert sender._socket is not None
    if not receiver.poll(1000):
        assert False, 'No message received'
    protocol, message = receiver.recv_multipart()
    assert protocol == b'streamapi/4'
    message = msgpack.unpackb(message, encoding='utf-8')
    assert message['ueid'] == str(entity.ueid)
    if not deleted:
        assert message['label'] == entity.label
    assert message['ttl'] == 120
    assert message.get('deleted', False) is deleted


def test_send_relationships(sender_receiver):
    sender, receiver = sender_receiver
    entity = entityd.EntityUpdate('MeType')
    entity.timestamp = 0
    entity.parents.add(cobe.UEID('a' * 32))
    entity.parents.add(cobe.UEID('b' * 32))
    entity.children.add(cobe.UEID('c' * 32))
    entity.children.add(cobe.UEID('d' * 32))
    sender.entityd_send_entity(entity)
    if not receiver.poll(1000):
        assert False, 'No message received'
    protocol, message = receiver.recv_multipart()
    assert protocol == b'streamapi/4'
    message = msgpack.unpackb(message, encoding='utf-8')
    parents = message.pop('parents')
    children = message.pop('children')
    assert message == {
        'ueid': str(entity.ueid),
        'type': 'MeType',
        'timestamp': 0,
        'attrs': {},
        'ttl': 120,
    }
    assert isinstance(parents, list)
    assert isinstance(children, list)
    assert set(parents) == {'a' * 32, 'b' * 32}
    assert set(children) == {'c' * 32, 'd' * 32}


@pytest.mark.parametrize('deleted', [True, False])
def test_send_label_unset(sender_receiver, deleted):
    sender, receiver = sender_receiver
    entity = entityd.EntityUpdate('MeType')
    entity.label = None
    entity.deleted = deleted
    sender.entityd_send_entity(entity)
    assert sender._socket is not None
    if not receiver.poll(1000):
        assert False, "No message received"
    protocol, message = receiver.recv_multipart()
    assert protocol == b'streamapi/4'
    message = msgpack.unpackb(message, encoding='utf-8')
    assert 'label' not in message


def test_wrong_server_certificate(sender_receiver, request, certificates):
    keypath = certificates.join('entityd/keys')
    keypath.join('modeld.key').rename(keypath.join('modeld.bkp'))
    keypath.join('entityd.key').rename(keypath.join('modeld.key'))

    def fin():
        keypath.join('modeld.key').rename(keypath.join('entityd.key'))
        keypath.join('modeld.bkp').rename(keypath.join('modeld.key'))
    request.addfinalizer(fin)

    sender, receiver = sender_receiver
    entity = entityd.EntityUpdate('MeType')
    entity.label = 'entity label'
    sender.entityd_send_entity(entity)
    assert sender._socket is not None
    assert receiver.poll(100) == 0


def test_unknown_client(request, certificates):
    keypath = certificates.join('modeld/keys')
    keypath.join('entityd.key').rename(keypath.join('entityd.bkp'))

    def fin():
        keypath.join('entityd.bkp').rename(keypath.join('entityd.key'))
    request.addfinalizer(fin)

    receiver = get_receiver('tcp://*:*', request, keypath)
    sender = get_sender(
        receiver.LAST_ENDPOINT,
        pathlib.Path(str(certificates.join('entityd/keys')))
        )
    entity = entityd.EntityUpdate('MeType')
    entity.label = 'entity label'
    sender.entityd_send_entity(entity)
    assert sender._socket is not None
    assert receiver.poll(100) == 0


def test_no_auth(request, certificates):
    receiver = get_receiver('tcp://*:*', request, keydir=None)
    keydir = certificates.join('entityd', 'keys')
    sender = get_sender(receiver.LAST_ENDPOINT, keydir)
    entity = entityd.EntityUpdate('MeType')
    entity.label = 'entity label'
    sender.entityd_send_entity(entity)
    assert sender._socket is not None
    assert receiver.poll(100) == 0


def test_send_unserializable(sender):
    entity = object()
    with pytest.raises(TypeError):
        sender.entityd_send_entity(entity)


def test_buffers_full(loghandler, sender):
    entity = {'uuid': 'abcdef'}
    for _ in range(501):
        sender.entityd_send_entity(entity)
    assert loghandler.has_warning(
        re.compile(r'Could not send, message buffers are full'))
    assert sender._socket is None


def test_attribute():
    entity = entityd.EntityUpdate('Type')
    entity.attrs.set('attr', 1, {'metric:counter'})
    encoded = entityd.mesend.MonitoredEntitySender.encode_entity(entity)
    decoded = msgpack.unpackb(encoded, encoding='utf8')
    assert decoded['attrs']['attr']['value'] == 1
    assert decoded['attrs']['attr']['traits'] == ['metric:counter']
    assert 'deleted' not in decoded['attrs']['attr']


def test_deleted_attribute():
    entity = entityd.EntityUpdate('Type')
    entity.attrs.delete('deleted')
    encoded = entityd.mesend.MonitoredEntitySender.encode_entity(entity)
    decoded = msgpack.unpackb(encoded, encoding='utf8')
    assert decoded['attrs']['deleted']['deleted'] is True
