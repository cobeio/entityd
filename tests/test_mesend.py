import argparse
import struct

import msgpack
import pytest
import zmq

import entityd
import entityd.mesend


@pytest.fixture
def sender():
    session = pytest.Mock()
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    session.config.args.dest = 'tcp://127.0.0.1:25010'
    return sender


@pytest.fixture
def sender_receiver(request):
    """Get an ME Sender with a matched receiving socket with random port."""
    context = zmq.Context()
    sock = context.socket(zmq.PULL)
    port_selected = sock.bind_to_random_port('tcp://127.0.0.1')

    # Must call sock.close first, else context.term will block.
    def term():
        sock.close()
        context.term()
    request.addfinalizer(term)
    session = pytest.Mock()
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    session.config.args.dest = 'tcp://127.0.0.1:{}'.format(
        port_selected)
    return sender, sock


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


def test_sessionstart():
    session = pytest.Mock()
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    assert sender.session == session
    assert isinstance(sender.context, zmq.Context)


def test_sessionfinish():
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(pytest.Mock())
    sender.socket = pytest.Mock()
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
    assert sender.socket is not None
    protocol, message = receiver.recv_multipart()
    protocol = struct.unpack('!I', protocol)[0]
    assert protocol == 1
    message = msgpack.unpackb(message, encoding='utf-8')
    assert message['ueid'] == entity.ueid
    if not deleted:
        assert message['label'] == entity.label
    assert message.get('deleted', False) is deleted


def test_send_unserializable(sender):
    entity = object()
    with pytest.raises(TypeError):
        sender.entityd_send_entity(entity)


def test_buffers_full(caplog, sender):
    entity = {'uuid': 'abcdef'}
    for _ in range(501):
        sender.entityd_send_entity(entity)
    assert [rec for rec in caplog.records() if
            rec.levelname == 'WARNING' and
            'Could not send, message buffers are full' in rec.msg]
    assert sender.socket is None


def test_attribute():
    entity = entityd.EntityUpdate('Type')
    entity.attrs.set('attr', 1, 'perf:counter')
    encoded = entityd.mesend.MonitoredEntitySender.encode_entity(entity)
    decoded = msgpack.unpackb(encoded, encoding='utf8')
    assert decoded['attrs']['attr']['value'] == 1
    assert decoded['attrs']['attr']['type'] == 'perf:counter'
    assert 'deleted' not in decoded['attrs']['attr']


def test_deleted_attribute():
    entity = entityd.EntityUpdate('Type')
    entity.attrs.delete('deleted')
    encoded = entityd.mesend.MonitoredEntitySender.encode_entity(entity)
    decoded = msgpack.unpackb(encoded, encoding='utf8')
    assert decoded['attrs']['deleted']['deleted'] is True
