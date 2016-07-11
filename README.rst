Entityd Monitoring Agent for Cobe.io
====================================

A daemon which monitors entities on a host system and streams them to
cobe.io.


Installation
============

It is recommended to get a binary conda-based distribution from
https://cobe.io which provides the entire Python environment
required.  Once you have created a Topology Instance there it will
offer to download an agent and provide installation instructions.

However if you prefer you can install from PyPI as well, a few more
manual steps are needed.  The entityd package relies on some other
PyPI packages which may require install-time compilation depending on
your setup.  If your system has everything available then installation
is as simple as::

   pip install entityd

Alternatively you may prefer to install into a virtualenv created just
for entityd, the pipsi_ project makes this very convenient::

   pipsi install entityd

.. _pipsi: https://github.com/mitsuhiko/pipsi


Configuration
-------------

To securely communicate with cobe.io you need to install encryption
keys which you can download from the agent installation page on
cobe.io.  There are two files to download, ``modeld.key`` and
``entityd.key_secret``, which need to be installed into an
``etc/entityd/keys/`` directory.  If you installed entityd into a
virtualenv, as recommended, then this directory is relative inside the
virtualenv, e.g.: ``/path/to/venv/etc/entityd/keys/``.  You may have
to create the directory before moving the keys there.


Systemd
-------

Once installed you probably want to ensure it gets started
automatically.  This will depend on the init system in use by your
host.

If you are using systemd_ you can install the following unit file to
``/etc/systemd/system/entityd.service``, being sure to provide the
correct path to the installed binary and the correct destination for
your Topology Instance::

   [Unit]
   Description=Entityd monitoring agent for cobe.io
   After=network.target

   [Service]
   Type=simple
   ExecStart=/usr/local/bin/entityd --dest=tcp://modeld.example.cobe.io:25010

   [Install]
   WantedBy=multi-user.target

.. _systemd: https://freedestop.org/wiki/Software/systemd/

Once the unit file is installed you can start entityd using
``systemctl start entityd.service``.  To automatically start it on
system boot you also need to execute ``sytemctl enable
entityd.service``.  If you modify the unit file be sure to execute
``systemctl daemon-reload`` to re-read the configuration.
