"""Plugins providing entities for Docker.

This module implements all the entities for various Docker
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""