"""Hook specifications for entityd plugins."""


import entityd.pm


@entityd.pm.hookdef(firstresult=True)
def entityd_main(pluginmanager, argv):
    """Invoke the main program.

    This hook is responsible for driving all other hooks.

    Return the exit code: 0 for success, 1 or greater for failure.

    """


@entityd.pm.hookdef
def entityd_plugin_registered(pluginmanager, name):
    """Called when a plugin is registerd.

    The primary usecase is to allow plugins to add new hooks via
    pluginmanager.addhooks(module).

    """


@entityd.pm.hookdef
def entityd_namespace():
    """Return dict of objects to be bound in the entityd namespace.

    This hook is called before command line options are parsed.
    """


@entityd.pm.hookdef
def entityd_addoption(parser):
    """Register argparse options.

    The parser is a argparse.ArgumentParser instance.

    """


@entityd.pm.hookdef(firstresult=True)
def entityd_cmdline_parse(pluginmanager, args):
    """Return initialised config object after parsing the arguments"""


@entityd.pm.hookdef
def entityd_configure(config):
    """Perform extra configuration.

    This hook is called after the command line has been parsed and
    plugins have been loaded, but before entityd_main is invoked.  It
    can be used to add new Managed Entities using the
    config.addentity() call.

    """


@entityd.pm.hookdef(firstresult=True)
def entityd_mainloop(config):
    """Implement the mainloop of the application."""


@entityd.pm.hookdef
def entityd_unconfigure(config):
    """Perform cleanup after entityd_main() as exited"""


@entityd.pm.hookdef
def entityd_find_entity(name, attrs=None):
    """Return an iterator of Monitored Entities.

    If the plugin does not provide the named ME type it should raise a
    LookupError.

    If ``attrs`` is given it must be a dictionary of attributes which
    should match.  This is a primitive way of filtering the Monitored
    Entities which will be returned by the iterator.  If value of an
    item in this dict is a compiled regular expression then the
    attribute must match this regular expression to be included in the
    iterator.

    """
