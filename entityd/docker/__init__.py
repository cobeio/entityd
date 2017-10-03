"""Plugins providing entities for Docker.

This module implements all the entities for various Docker
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""
import entityd.docker.container
import entityd.docker.swarm
import entityd.docker.daemon


def get_ueid(class_name, *args):
    """Get a ueid for any docker entity type."""
    values = {
        'DockerContainer': entityd.docker.container.DockerContainer,
        'DockerSwarm': entityd.docker.swarm.DockerSwarm,
        'DockerDaemon': entityd.docker.daemon.DockerDaemon,
        'DockerNetwork': entityd.docker.swarm.DockerNetwork,
        'DockerNode': entityd.docker.swarm.DockerNode,
    }
    return values[class_name].get_ueid(*args)
