"""Plugin providing MySQL monitored entities"""

import itertools

import entityd.pm


class MySQLEntity:
    """Monitor for MySQL instances.

       Multiple instances per-host - same kinds of problems as Apache.
       Different:
        - datadir, port, socket, basedir...


       What does an entity contain?

       Processes => mysqld
       mysql    20685  0.1  5.8 1069340 457056 ?      Ssl  16:00   0:01 /usr/sbin/mysqld

       Config files mysqld --print-defaults # gives a bunch of config defaults read from conf (
       may have been changed since mysql restart)

       Do we handle mariadb as well as mysql? Looks like maria uses mysql binaries (on centos)
       but different locs for logs, /var/pid etc

       Authentication is a big deal if we want to get any more monitoring information out.
       - Otherwise, there's nothing here that we can't get from Process/Endpoint/File info
         (Except maybe files, which we're not sending... Could be a declentity?)

       If we did have an authenticated user - we could do things like 'SHOW STATUS'
       which gives lots of useful info e.g.
       Bytes_received, Aborted_connects, Uptime etc etc

       Without this, a specialised entity may be a little bit pointless. May just need "process"
       privilege

       Datadir?

       # Connected clients should be possible from connections, knowing where we're listening

        # Creating a mysql user for entityd:
        mysql> create user 'entityd'@'localhost' identified by 'entityd';
        Query OK, 0 rows affected (0.35 sec)
        ## This grant might not even be needed actually
        mysql> grant process on *.* to 'entityd'@'localhost';
        Query OK, 0 rows affected (0.00 sec)

        $ echo 'show status' | mysql -u entityd -pentityd or $ mysqladmin status -uentityd
        -pentityd # can also do extended-status Uptime: 412502  Threads: 1  Questions: 947  Slow
        queries: 0  Opens: 758  Flush tables: 1  Open tables: 80  Queries per second avg: 0.002

        This gives a tonne of info. Who decides what we send? Could be a massive
        entity. Apache just sends everything. At least there's not going to be
        loads of mysql instances (probably).

        Privileges on centos seem odd; a new user can read all the tables?

        Assumptions:
         - The MySQL process is called mysqld
    """

    def __init__(self):
        self.session = None
        self._host_ueid = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the MySQL Monitored Entity."""
        config.addentity('MySQL', 'entityd.mysqlme.MySQLEntity')

    @entityd.pm.hookimpl()
    def entityd_sessionstart(self, session):
        """Store session for later use."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):
        """Return an iterator of "MySQL" Monitored Entities."""
        if name == 'MySQL':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported '
                                  'for attrs {}'.format(attrs))
            return self.entities(include_ondemand=include_ondemand)

    @property
    def host_ueid(self):
        """Get and store the host ueid."""
        if self._host_ueid:
            return self._host_ueid
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Host', attrs=None)
        for hosts in results:
            for host in hosts:
                self._host_ueid = host.ueid
                return self._host_ueid

    def entities(self, include_ondemand):
        """Return MySQLEntity objects."""
        for proc in self.top_level_mysql_processes():
            mysql = MySQL(proc)
            update = entityd.EntityUpdate('MySQL')
            update.attrs.set('host', self.host_ueid, attrtype='id')
            update.attrs.set('config_path', mysql.config_path(), attrtype='id')
            update.attrs.set('process_id', proc.attrs.get('pid').value)
            if include_ondemand:
                files = list(itertools.chain.from_iterable(
                    self.session.pluginmanager.hooks.entityd_find_entity(
                        name='File', attrs={'path': mysql.config_path()})
                ))
                if files:
                    update.children.add(files[0])
                    yield files[0]
            update.children.add(proc)
            update.parents.add(self.host_ueid)
            yield update

    def top_level_mysql_processes(self):
        """Find top level MySQL processes.

        Assumes that we are looking for processes named 'mysqld'.
        :return: List of Process ``EntityUpdate``s whose parent is not
           also 'mysqld'.
        """
        processes = {}
        proc_gens = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': 'mysqld'})
        for entity in itertools.chain.from_iterable(proc_gens):
            processes[entity.attrs.get('pid').value] = entity
        return [e for e in processes.values()
                if e.attrs.get('ppid').value not in processes]


class MySQL:
    """Abstract MySQL instance.

    :ivar process: The main MySQL process as an EntityUpdate
    """

    def __init__(self, process):
        self.process = process

    @staticmethod
    def config_path():
        """Get the path for our my.cnf"""
        return '/etc/mysql/my.cnf'  # TODO: This shouldn't be hardcoded
