import argparse

import pytest
import zmq

import entityd.mesend


@pytest.fixture
def sender():
    session = pytest.Mock()
    entity = {'uuid': 'abcdef'}
    sender = entityd.mesend.MonitoredEntitySender()
    sender.entityd_sessionstart(session)
    session.config.args.dest = 'tcp://127.0.0.1:25010'
    return sender


def test_plugin_registered(pm):
    name = 'entityd.mesend'
    entityd.mesend.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.mesend.MonitoredEntitySender')


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
    sender.entityd_sessionfinish()
    sender.socket.close.assert_called_once_with(linger=500)


def test_send_entity(sender):
    entity = {'uuid': 'abcdef'}
    sender.entityd_send_entity(entity)
    assert sender.socket is not None


def test_send_unserializable(caplog, sender):
    entity = object()
    sender.entityd_send_entity(entity)
    errors = [rec for rec in caplog.records() if rec.levelname == 'ERROR']
    assert len(errors) == 1
    assert 'Cannot serialize entity' in errors[0].msg


def test_buffers_full(caplog):
    entity = {'uuid': 'abcdef'}
    sender = entityd.mesend.MonitoredEntitySender()
    socket = pytest.Mock()
    sender.socket = socket
    sender.socket.send_multipart = pytest.Mock(side_effect=zmq.error.Again)

    sender.entityd_send_entity(entity)
    assert socket.close.called
    assert [rec for rec in caplog.records() if rec.levelname == 'WARNING']
    assert sender.socket is None
