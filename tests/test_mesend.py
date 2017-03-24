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
    # Create a receiver socket, initialise it with default options and
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
    session.config.args.dest = endpoint
    session.config.keydir = pathlib.Path(str(keydir))
    session.config.args.stream_optimise = False
    session.config.args.stream_optimise_frequency = 1
    sender.entityd_sessionstart(session)
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


@pytest.mark.parametrize('optimised', [True, False])
@pytest.mark.parametrize(
    ('optimised_frequency', 'optimised_frequency_expected'),
    [
        (-1, 1),
        (0, 1),
        (1, 1),
        (5, 5),
    ],
)
def test_sessionstart(
        optimised, optimised_frequency, optimised_frequency_expected):
    session = pytest.Mock()
    session.config.args.stream_optimise = optimised
    session.config.args.stream_optimise_frequency = optimised_frequency
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    assert sender.session == session
    assert isinstance(sender.context, zmq.Context)
    assert sender._optimised == optimised
    assert sender._optimised_cycles_max == optimised_frequency_expected


def test_sessionfinish():
    session = pytest.Mock()
    session.config.args.stream_optimise = False
    session.config.args.stream_optimise_frequency = 5
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    sender._socket = pytest.Mock()
    context = sender.context
    sender.entityd_sessionfinish()
    sender.socket.close.assert_called_once_with(linger=500)
    assert context.closed


@pytest.mark.parametrize('exists', [True, False])
def test_send_entity(sender_receiver, exists):
    sender, receiver = sender_receiver
    entity = entityd.EntityUpdate('MeType')
    entity.label = 'entity label'
    if not exists:
        entity.set_not_exists()
    sender.entityd_send_entity(entity)
    assert sender._socket is not None
    if not receiver.poll(1000):
        assert False, 'No message received'
    protocol, message = receiver.recv_multipart()
    assert protocol == b'streamapi/5'
    message = msgpack.unpackb(message, encoding='utf-8')
    assert message['ueid'] == str(entity.ueid)
    if exists:
        assert message['label'] == entity.label
    assert message['ttl'] == 120
    assert message.get('exists', True) is exists


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
    assert protocol == b'streamapi/5'
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
    assert protocol == b'streamapi/5'
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


class TestUpdateOptimisation:

    @pytest.fixture()
    def sender(self):
        session = pytest.Mock()
        sender = entityd.mesend.MonitoredEntitySender()
        session.config.args.stream_optimise = True
        session.config.args.stream_optimise_frequency = 1000
        sender.entityd_sessionstart(session)
        return sender

    def test_remove_duplicate(self, sender):
        update_0 = entityd.EntityUpdate('Foo')
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_1 = entityd.EntityUpdate('Foo')
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        assert {attribute.name for attribute in update_0.attrs} == {'id'}
        assert {attribute.name for attribute in update_1.attrs} == set()
        assert update_0.ueid == update_1.ueid

    def test_send_after_delete(self, sender):
        update_0 = entityd.EntityUpdate('Foo')
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_0.attrs.set('tobe', 'ornot', {'tobe'})
        update_1 = entityd.EntityUpdate('Foo')
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        update_1.attrs.delete('tobe')
        update_2 = entityd.EntityUpdate('Foo')
        update_2.attrs.set('id', 'snowflake', {'entity:id'})
        update_2.attrs.set('tobe', 'ornot', {'tobe'})
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        sender._optimise_update(update_2)
        assert {
            attribute.name for attribute in update_0.attrs} == {'id', 'tobe'}
        assert {attribute.name for attribute in update_1.attrs} == set()
        assert {attribute.name for attribute in update_2.attrs} == {'tobe'}
        assert update_0.ueid == update_1.ueid == update_2.ueid

    def test_send_if_values_change(self, sender):
        update_0 = entityd.EntityUpdate('Foo')
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_0.attrs.set('changes', 'initial', set())
        update_1 = entityd.EntityUpdate('Foo')
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        update_1.attrs.set('changes', 'changed', set())
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        assert {
            attribute.name for attribute in update_0.attrs} == {'id', 'changes'}
        assert {attribute.name for attribute in update_1.attrs} == {'changes'}
        assert update_0.ueid == update_1.ueid

    def test_send_if_traits_change(self, sender):
        update_0 = entityd.EntityUpdate('Foo')
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_0.attrs.set('changes', '...', {'initial'})
        update_1 = entityd.EntityUpdate('Foo')
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        update_1.attrs.set('changes', '...', {'initial', 'changed'})
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        assert {
            attribute.name for attribute in update_0.attrs} == {'id', 'changes'}
        assert {attribute.name for attribute in update_1.attrs} == {'changes'}
        assert update_0.ueid == update_1.ueid

    def test_send_after_exists_false(self, sender):
        update_0 = entityd.EntityUpdate('Foo')
        update_0.exists = True
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_1 = entityd.EntityUpdate('Foo')
        update_1.exists = False
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        update_2 = entityd.EntityUpdate('Foo')
        update_2.exists = True
        update_2.attrs.set('id', 'snowflake', {'entity:id'})
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        sender._optimise_update(update_2)
        assert {attribute.name for attribute in update_0.attrs} == {'id'}
        assert {attribute.name for attribute in update_1.attrs} == set()
        assert {attribute.name for attribute in update_2.attrs} == {'id'}
        assert update_0.ueid == update_1.ueid == update_2.ueid

    def test_send_all_after_limit(self, sender):
        sender._optimised_cycles_max = 1
        update_0 = entityd.EntityUpdate('Foo')
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_1 = entityd.EntityUpdate('Foo')
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        assert {attribute.name for attribute in update_0.attrs} == {'id'}
        assert {attribute.name for attribute in update_1.attrs} == {'id'}
        assert update_0.ueid == update_1.ueid

    def test_send_all_if_disabled(self, sender):
        sender._optimised = False
        update_0 = entityd.EntityUpdate('Foo')
        update_0.attrs.set('id', 'snowflake', {'entity:id'})
        update_1 = entityd.EntityUpdate('Foo')
        update_1.attrs.set('id', 'snowflake', {'entity:id'})
        sender._optimise_update(update_0)
        sender._optimise_update(update_1)
        assert {attribute.name for attribute in update_0.attrs} == {'id'}
        assert {attribute.name for attribute in update_1.attrs} == {'id'}
        assert update_0.ueid == update_1.ueid
