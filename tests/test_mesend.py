import pytest
import zmq

import entityd.mesend


def test_plugin_registered():
    pm = pytest.Mock()
    name = 'entityd.mesend'
    entityd.mesend.entityd_plugin_registered(pm, name)
    assert pm.register.called_once()
    assert isinstance(pm.register.mock_calls[0][1][0],
                      entityd.mesend.MonitoredEntitySender)
    assert pm.register.mock_calls[0][2]['name'] == \
        'entityd.mesend.MonitoredEntitySender'


def test_addoption():
    parser = pytest.Mock()
    entityd.mesend.MonitoredEntitySender().entityd_addoption(parser)
    assert parser.add_argument.called_once()
    assert parser.mock_calls[0][1][0] == '--dest'
    named_args = parser.mock_calls[0][2]
    assert named_args['default'] == 'tcp://127.0.0.1:25010'
    assert named_args['type'] == str
    assert named_args['help'] == 'ZeroMQ address of modeld destination.'


def test_sessionstart():
    session = pytest.Mock()
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    assert sender.session == session
    assert sender.config == session.config
    assert isinstance(sender.context, zmq.Context)


def test_sessionfinish():
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(pytest.Mock())
    sender.socket = pytest.Mock()
    sender.entityd_sessionfinish()
    assert sender.socket.close.called_once_with(linger=1)


def test_send_entity():
    session = pytest.Mock()
    entity = {'uuid': 'abcdef'}
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    session.config.args.dest = 'tcp://127.0.0.1:25010'
    sender.entityd_send_entity(session, entity)
    assert sender.socket is not None


def test_buffers_full():
    entity = {'uuid': 'abcdef'}
    sender = entityd.mesend.MonitoredEntitySender()
    socket = pytest.Mock()
    sender.socket = socket
    sender.socket.send_multipart = pytest.Mock(side_effect=zmq.error.Again)
    entityd.mesend.log = pytest.Mock()

    sender.entityd_send_entity(None, entity)
    assert socket.close.called
    assert entityd.mesend.log.warning.called
    assert sender.socket is None



