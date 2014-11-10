"""Hook specifications for entityd plugins."""

# pylint: disable=unused-argument


import entityd.pm


@entityd.pm.hookdef(firstresult=True)
def entityd_main(pluginmanager, argv):
    """Invoke the main program.

    This hook is responsible for driving all other hooks.

    Return the exit code: 0 for success, 1 or greater for failure.

    """


@entityd.pm.hookdef
def entityd_plugin_registered(pluginmanager, name):
    """Called when a plugin is registered.

    The primary usecase is to allow plugins to add new hooks via
    pluginmanager.addhooks(module) or to register new plugins before
    any other hooks are called.

    """


@entityd.pm.hookdef
def entityd_namespace():
    """Return dict of objects to be bound in the entityd namespace.

    This hook is called before command line options are parsed.

    XXX Not yet implemented.

    """


@entityd.pm.hookdef(firstresult=True)
def entityd_cmdline_parse(pluginmanager, argv):
    """Return initialised config object after parsing the arguments.

    This hook is responsible for calling the entityd_addoption hook.
    """


@entityd.pm.hookdef
def entityd_addoption(parser):
    """Register argparse options.

    The parser is a argparse.ArgumentParser instance.

    """


@entityd.pm.hookdef
def entityd_configure(config):
    """Perform extra configuration.

    This hook is called after the command line has been parsed and
    plugins have been loaded, but before entityd_main is invoked.  It
    can be used to add new Managed Entities using the
    config.addentity() call.

    """


@entityd.pm.hookdef
def entityd_sessionstart(session):
    """A monitoring session has been created.

    This hook is called after entityd is configured and a Session has
    been instantiated.

    """


@entityd.pm.hookdef(firstresult=True)
def entityd_mainloop(session):
    """Implement the mainloop of the application."""


@entityd.pm.hookdef
def entityd_sessionfinish(session):
    """A monitoring session has been finished.

    This hook is called when the Session instance is about to be
    destroyed.

    """


@entityd.pm.hookdef
def entityd_unconfigure(config):
    """Perform cleanup after entityd_mainloop() as exited."""


@entityd.pm.hookdef
def entityd_find_entity(name, attrs=None):
    """Return an iterator of Monitored Entities.

    If there are no entities matching the provided name or attributes
    then either ``None`` or an empty iterator is returned.

    If ``attrs`` is given it must be a dictionary of attributes which
    should match.  This is a primitive way of filtering the Monitored
    Entities which will be returned by the iterator.  If value of an
    item in this dict is a compiled regular expression then the
    attribute must match this regular expression to be included in the
    iterator.

    """


@entityd.pm.hookdef
def entityd_send_entity(session, entity):
    """Send a Monitored Entity to a modeld destination."""


@entityd.pm.hookdef(firstresult=True)
def entityd_kvstore_add(key, value):
    """Persist this key -> value mapping."""


@entityd.pm.hookdef(firstresult=True)
def entityd_kvstore_addmany(values):
    """Persist these mappings.

    :param values: A dictionary of keys and values to store
    """


@entityd.pm.hookdef(firstresult=True)
def entityd_kvstore_get(key):
    """Retrieve the value for ``key``."""


@entityd.pm.hookdef(firstresult=True)
def entityd_kvstore_getmany(key_begins_with):
    """Retrieve rows which start with ``key_begins_with``."""


@entityd.pm.hookdef(firstresult=True)
def entityd_kvstore_delete(key):
    """Delete the mapping for ``key``."""


@entityd.pm.hookdef(firstresult=True)
def entityd_kvstore_deletemany(key_begins_with):
    """Delete all mappings beginning with ``key_begins_with``."""
