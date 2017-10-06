"""Plugin providing the Host Monitored Entity."""

import os
import platform
import socket
import threading

import act
import logbook
import syskit
import zmq

import entityd.pm


class HostCpuUsage(threading.Thread):
    """A background thread fetching cpu times and calculating percentages.

    Accessible via ZMQ Pair/Pair socket; receives a
    request and responds with  a list of tuples containing the required
    attribute values: (name, value, traits)

    :param Context context: The ZMQ context to use
    :param str endpoint: The ZMQ endpoint to listen for requests on
    :param int interval: The period in seconds to wait between refreshes

    :ivar last_cpu_times: Last cpu times reported from syskit
    :ivar last_attributes: The last entity attributes constructed
    """

    def __init__(self, context, endpoint='inproc://hostcpuusage', interval=15):
        self._context = context
        self.listen_endpoint = endpoint
        self.last_cpu_times = None
        self.last_attributes = []
        self._stream = None
        self._timer_interval = interval
        self._log = logbook.Logger('HostCpuUsage')
        super().__init__()

    def _update_times(self):
        """Get current cpu times and update known attributes."""
        attrs = ['usr', 'nice', 'sys', 'idle', 'iowait', 'irq', 'softirq',
                 'steal', 'guest', 'guest_nice']
        new_cputimes = syskit.cputimes()
        attributes = []
        for attr in attrs:
            attributes.append((attr, float(getattr(new_cputimes, attr)),
                               {'time:duration', 'unit:seconds'}))
        if self.last_cpu_times:
            diffs = [x - y for x, y in zip(new_cputimes, self.last_cpu_times)]
            total = sum(diffs)
            if total != 0:
                attributes.extend([('cpu:' + attr,
                                    float(diff) / float(total) * 100,
                                    {'metric:gauge', 'unit:percent'})
                                   for attr, diff in zip(attrs, diffs)])
        self.last_cpu_times = new_cputimes
        self.last_attributes = attributes
        return attributes

    def run(self):
        """Run in a loop, restarting on unexpected exceptions."""
        while True:
            try:
                self._run()
            except Exception:  # pylint: disable=broad-except
                self._log.exception('An unexpected exception occurred in '
                                    'HostCpuUsage thread')
            else:
                break
            finally:
                self.stop()

    def _run(self):
        """Run the thread main loop.

        Registers the regular timer and polls for
        events from the incoming request socket.
        """
        self._stream = act.zkit.EventStream(self._context)
        timer = act.zkit.SimpleTimer()
        timer.schedule(0)
        sock = self._context.socket(zmq.PAIR)
        sock.bind(self.listen_endpoint)
        self._stream.register(sock, self._stream.POLLIN)
        self._stream.register(timer, self._stream.TIMER)
        try:
            for event, _ in self._stream:
                if event is timer:
                    self._update_times()
                    timer.schedule(self._timer_interval * 1000)
                elif event is sock:
                    _ = sock.recv_pyobj()
                    sock.send_pyobj(self.last_attributes)
        finally:
            sock.close(linger=0)
            self._stream.close()

    def stop(self):
        """Stop the thread safely."""
        if self._stream:
            self._stream.send_term()


class HostEntity:                    # pylint: disable=too-many-instance-attributes
    """Plugin to generate Host MEs."""

    def __init__(self):
        self.host_uuid = None
        self.session = None
        self._bootid = None
        self._incontainer = None
        self.cpuusage_sock = None
        self.cpuusage_thread = None
        self.zmq_context = None

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Called when the monitoring session starts."""
        self.session = session
        self.zmq_context = act.zkit.new_context()
        self.cpuusage_thread = HostCpuUsage(self.zmq_context)
        self.cpuusage_thread.start()
        self.cpuusage_sock = self.zmq_context.socket(zmq.PAIR)
        self.cpuusage_sock.connect(self.cpuusage_thread.listen_endpoint)

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Finish the session.

        Stops the thread and closes the socket.
        """
        if self.cpuusage_thread:
            self.cpuusage_thread.stop()
            self.cpuusage_thread.join(timeout=2)
        if self.cpuusage_sock:
            self.cpuusage_sock.close(linger=0)
        self.zmq_context.destroy(linger=0)

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Host Monitored Entity."""
        config.addentity('Host', 'entityd.hostme.HostEntity')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of "Host" Monitored Entities."""
        if name == 'Host':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.hosts()

    @property
    def bootid(self):
        """Get and store the boot ID of the executing kernel.

        :returns: Kernel's boot ID UUID string.
        """
        if self._bootid:
            return self._bootid
        with open('/proc/sys/kernel/random/boot_id', 'r') as fp:
            self._bootid = fp.read().strip()
            return self._bootid

    @property
    def incontainer(self):
        """Get and store boolean of whether entityd is running in a container.

        That entityd is running in a container is identified by the
        presence of file `/.dockerenv`.

        :returns: Boolean of whether entityd is running in a container.
        """
        if self._incontainer is None:
            self._incontainer = os.path.isfile('/.dockerenv')
        return self._incontainer

    def hosts(self):
        """Generator of Host MEs."""
        update = entityd.EntityUpdate('Host')
        if not self.incontainer:
            hostname = socket.gethostname()
            update.label = hostname
            update.attrs.set('hostname', hostname)
            update.attrs.set('fqdn', socket.getfqdn())
        update.attrs.set('bootid', self.bootid, {'entity:id'})
        update.attrs.set('uptime', int(syskit.uptime()),
                         {'time:duration', 'unit:seconds', 'metric:counter'})
        update.attrs.set('boottime', syskit.boottime().timestamp(),
                         {'time:posix', 'unit:seconds'})
        load = syskit.loadavg()
        update.attrs.set('loadavg_1', load[0], {'metric:gauge'})
        update.attrs.set('loadavg_5', load[1], {'metric:gauge'})
        update.attrs.set('loadavg_15', load[2], {'metric:gauge'})
        memorystats = syskit.MemoryStats()
        free = memorystats.free + memorystats.buffers + memorystats.cached
        update.attrs.set('free', free * 1024,
                         {'unit:bytes', 'metric:gauge'})
        update.attrs.set('total', memorystats.total * 1024,
                         {'unit:bytes', 'metric:gauge'})
        update.attrs.set('used', (memorystats.total - free) * 1024,
                         {'unit:bytes', 'metric:gauge'})
        update.attrs.set('os', platform.system())
        update.attrs.set('osversion', platform.release())
        self._add_cputime_attrs(update)
        yield update

    def _add_cputime_attrs(self, update):
        """Add cputimes and their % values to update.

        The first call will return values since system boot; subsequent calls
        will return the values for the period in between calls.
        """
        self.cpuusage_sock.send_pyobj('REQ')
        cpupc = self.cpuusage_sock.recv_pyobj()
        for attr, val, traits in cpupc:
            update.attrs.set(attr, val, traits)
