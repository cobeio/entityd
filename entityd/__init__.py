"""The Abilisoft Entity Monitoring Agent.

This package contains entityd, an agent which collects host health,
performance and topology information and sends it to Abilisoft's
modeld so the topolgy engine can build a topology off the entire
infrastructure and perform alerting and reporting on this..

"""

# Include EntityUpdate in entityd namespace until entityd_namespace is
# implemented.
from . import entityupdate
EntityUpdate = entityupdate.EntityUpdate
