"""Health checks for Entityd."""

import pathlib
import sys

import act


_PATH_HEALTH = pathlib.Path(str(act.fsloc.statedir.joinpath('healthy')))


def heartbeat():
    """Mark Entityd has being healthy.

    This may be called many times consecutively.
    """
    _PATH_HEALTH.touch()


def die():
    """Mark Entityd as being dead.

    This may be called many times consecutively.
    """
    try:
        _PATH_HEALTH.unlink()
    except FileNotFoundError:
        pass


def check():
    """Check if Entityd is marked as healthy.

    This will remove the health marker so that subsequent invocations
    will signify that Entityd is dead unless it has made a call to
    :func:`heartbeat`.

    :raises SystemExit: With the exit code set to zero if Entityd is
        healthy. Otherwise the exit code will be one.
    """
    healthy = _PATH_HEALTH.is_file()
    try:
        _PATH_HEALTH.unlink()
    except FileNotFoundError:
        pass
    raise SystemExit(int(not healthy))
