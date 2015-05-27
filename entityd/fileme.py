"""An entity for files.

Does not return anything for entity collection;
only returns explicitly requested files.
"""

import logging
import os
import stat

import entityd


class FileEntity:
    """Entity for a local file.

    This is an on demand entity. That is, by itself, no File entities will
    be emitted. Other entities may explicitly request File entities in order
    to include them as part of their relations.
    """

    def __init__(self):
        self.session = None
        self._host_ueid = None
        self.log = logging.getLogger(__name__)

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the File Monitored Entity."""
        config.addentity('File', 'entityd.fileme.FileEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of "File" Monitored Entities.

        An empty iterator will be returned if attrs is empty, or if the attrs
        provided do not match an existing file.
        """
        if name == 'File':
            if attrs is None:
                return iter([])
            else:
                return self.filtered_entities(attrs)

    def filtered_entities(self, attrs):
        """Create and yield file entities matching the attrs provided.

        If no files match the attributes provided then we will return an empty
        iterator.

        :param attrs: Must be supplied to get entities back. Only `path` is
           supported and `path` must be set to the absolute location of an
           existing file.

        :return: Iterator of file entities.
        """
        if 'path' in attrs:
            if os.path.isfile(attrs['path']):
                yield self.create_entity(path=attrs['path'])
            else:
                self.log.debug('Failed to create entity for non-existent file\
                                at %s', attrs['path'])

    @property
    def host_ueid(self):
        """Property to get the host ueid, used in a few places"""
        if not self._host_ueid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            if results:
                host_me = next(iter(results[0]))
                self._host_ueid = host_me.ueid
        return self._host_ueid

    def create_entity(self, path):
        """Create a File EntityUpdate."""
        fstat = os.stat(path)
        update = entityd.EntityUpdate('File')
        update.label = path
        update.attrs.set('host', self.host_ueid, attrtype='id')
        update.attrs.set('path', path, attrtype='id')
        update.attrs.set('uid', fstat.st_uid)
        update.attrs.set('gid', fstat.st_gid)
        update.attrs.set('permissions', stat.filemode(fstat.st_mode))
        update.attrs.set('lastmodified', fstat.st_mtime, attrtype='perf:counter')
        update.attrs.set('size', fstat.st_size, attrtype='perf:gauge')
        return update