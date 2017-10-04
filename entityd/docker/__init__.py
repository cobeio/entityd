"""This package contains all the docker entities."""


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
        'DockerNetwork': entityd.docker.swarm.DockerNetwork,
        'DockerNode': entityd.docker.swarm.DockerNode,
    }
    return values[class_name].get_ueid(*args)
