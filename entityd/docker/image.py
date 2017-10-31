"""Plugin to provide entities for Docker images."""

import collections

import logbook

import entityd
import entityd.docker.client
import entityd.docker.daemon


log = logbook.Logger(__name__)


class DockerImage:
    """Entity for a Docker Container."""

    def __init__(self):
        self._images = {}  # digest : image

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker image entity updates."""
        yield from self._generate_images()
        yield from self._generate_labels()

    @entityd.pm.hookimpl
    def entityd_collection_before(self, session):  # pylint: disable=unused-argument
        """Collect images from available Docker daemon.

        If no connection to a Docker daemon is available this
        does nothing.
        """
        if not entityd.docker.client.DockerClient.client_available():
            return
        client = entityd.docker.client.DockerClient.get_client()
        for image in client.images.list(all=True):
            self._images[image.id] = image

    @entityd.pm.hookimpl
    def entityd_collection_after(self, session, updates):  # pylint: disable=unused-argument
        """Clear images that were collected during collection."""
        self._images.clear()

    @classmethod
    def get_ueid(cls, digest):
        """Get the UEID for a Docker image.

        :param digest: Full image digest including the hash component.
        :type digest: str

        :returns: :class:`cobe.UEID` for the Docker image.
        """
        update = entityd.EntityUpdate('Docker:Image')
        update.attrs.set('digest', digest, {'entity:id'})
        return update.ueid

    def _image_label(self, update, image):
        """Determine the label to use for an image.

        This will modify the given update so that it has an appropriate
        label to represent the given Docker image.

        By default the digest ID of the image is used, with the hash
        function component removed and then truncated to 12 characters.
        This is consistent with Docker's default command line behaviour.

        If the image as a repository tag, then it will be used
        instead. If there are multiple repository tags, they are
        sorted lexicographically, with the lowest order one being
        used.

        If the image is tagged as ``latest`` then it is ignored by
        default, giving prescedence to non-latest tags. However, if
        there are only ``latest`` tags, then they will be used.

        :param update: Docker image update to give a label to.
        :type update: entityd.EntityUpdate
        :param image: Docker image to use to determine the label.
        :type image: docker.models.image.Image
        """
        update.label = image.id.split(':', 1)[-1][:12]
        tags = sorted(image.attrs['RepoTags'], reverse=True)
        tags_filtered = [tag for tag in tags if not tag.endswith(':latest')]
        if tags:
            update.label = tags[0]
            if tags_filtered:
                update.label = tags_filtered[0]

    def _generate_images(self):
        """Generate updates for all Docker images.

        :returns: Iterator of ``Docker:Image`` updates.
        """
        for image in self._images.values():
            update = entityd.EntityUpdate('Docker:Image')
            self._image_label(update, image)
            self._image_parents(update, image)
            update.attrs.set('digest', image.id, {'entity:id'})
            update.attrs.set(
                'created', image.attrs['Created'], {'time:rfc3339'})
            update.attrs.set('size', image.attrs['Size'], {'unit:bytes'})
            update.attrs.set(
                'size:virtual', image.attrs['VirtualSize'], {'unit:bytes'})
            update.attrs.set('architecture', image.attrs['Architecture'])
            update.attrs.set('operating-system', image.attrs['Os'])
            update.attrs.set('docker:version', image.attrs['DockerVersion'])
            update.attrs.set(
                'docker:driver', image.attrs['GraphDriver']['Name'])
            update.attrs.set(
                'entry-point', image.attrs['Config']['Entrypoint'])
            update.attrs.set('command', image.attrs['Config']['Cmd'])
            update.attrs.set('dangling', not image.attrs['RepoTags'])
            yield update

    def _image_parents(self, update, image):
        """Add parent to a Docker image upate.

        This takes the parent of the given image and attempts to add
        the appropriate UEID as a parent of the given update.

        The parent image of an image is the image that built the
        image. The parent image may be a foreign relation or not
        present at all.

        :param update: Docker image update to add a parent to.
        :type update: entityd.EntityUpdate
        :param image: Docker image to use to determine the parent.
        :type image: docker.models.image.Image
        """
        parent_digest = image.attrs['Parent']
        if parent_digest:
            update.parents.add(self.get_ueid(parent_digest))

    def _generate_labels(self):
        """Generate updates for all Docker label.

        This scans all the Docker images for labels and creates an
        update for each unique label that is found. Each label is
        identified by its key and value.

        All images that share the same label are added as children
        of the label update.

        :returns: Iterator of ``Group`` updates.
        """
        labels = collections.defaultdict(set)  # (key, value) : image digest
        for image in self._images.values():
            labels_image = image.attrs['Config']['Labels'] or {}
            for label_pair in labels_image.items():
                labels[label_pair].add(image.id)
        for (label_key, label_value), images in labels.items():
            update = entityd.EntityUpdate('Group')
            update.label = "{0} = {1}".format(label_key, label_value)
            update.attrs.set('kind', 'label:' + label_key, {'entity:id'})
            update.attrs.set('id', label_value, {'entity:id'})
            for image in images:
                update.children.add(self.get_ueid(image))
            yield update
