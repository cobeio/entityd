"""Plugins providing entities for Docker.

This module implements all the entities for various Docker
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""


def get_ueid(class_name, *args):
    """Get a ueid for any docker entity type."""
    import entityd.docker.container
    import entityd.docker.image
    import entityd.docker.swarm
    import entityd.docker.daemon
    values = {
        'DockerContainer': entityd.docker.container.DockerContainer,
        'DockerImage': entityd.docker.image.DockerImage,
        'DockerSwarm': entityd.docker.swarm.DockerSwarm,
        'DockerDaemon': entityd.docker.daemon.DockerDaemon,
        'DockerNode': entityd.docker.swarm.DockerNode,
    }
    return values[class_name].get_ueid(*args)
