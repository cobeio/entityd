"""
Code for retrieving connections. From psutil, and slightly modified for
our codebase.

https://github.com/giampaolo/psutil/blob/master/psutil/_pslinux.py
"""

import base64
import collections
import errno
import os
import socket
import struct
import sys

TCP_STATUSES = {
    '01': 'ESTABLISHED',
    '02': 'SYN_SENT',
    '03': 'SYN_RECV',
    '04': 'FIN_WAIT1',
    '05': 'FIN_WAIT2',
    '06': 'TIME_WAIT',
    '07': 'CLOSE',
    '08': 'CLOSE_WAIT',
    '09': 'LAST_ACK',
    '0A': 'LISTEN',
    '0B': 'CLOSING'
}


Connection = collections.namedtuple('Connection', ['fd', 'family', 'type',
                                                   'laddr', 'raddr',
                                                   'status', 'bound_pid'])


def pids():
    """Returns a list of PIDs currently running on the system."""
    return [int(x) for x in os.listdir('/proc') if x.isdigit()]


class Connections:
    """A wrapper on top of /proc/net/* files, retrieving per-process
    and system-wide open connections (TCP, UDP, UNIX) similarly to
    "netstat -an".

    Note: in case of UNIX sockets we're only able to determine the
    local endpoint/path, not the one it's connected to.
    According to [1] it would be possible but not easily.

    [1] http://serverfault.com/a/417946
    """

    def __init__(self):
        tcp4 = ("tcp", socket.AF_INET, socket.SOCK_STREAM)
        tcp6 = ("tcp6", socket.AF_INET6, socket.SOCK_STREAM)
        udp4 = ("udp", socket.AF_INET, socket.SOCK_DGRAM)
        udp6 = ("udp6", socket.AF_INET6, socket.SOCK_DGRAM)
        unix = ("unix", socket.AF_UNIX, None)
        self.tmap = {
            "all": (tcp4, tcp6, udp4, udp6, unix),
            "tcp": (tcp4, tcp6),
            "tcp4": (tcp4,),
            "tcp6": (tcp6,),
            "udp": (udp4, udp6),
            "udp4": (udp4,),
            "udp6": (udp6,),
            "unix": (unix,),
            "inet": (tcp4, tcp6, udp4, udp6),
            "inet4": (tcp4, udp4),
            "inet6": (tcp6, udp6),
            }

    @staticmethod
    def get_proc_inodes(pid):
        """Gets all inodes for the given process.

        :param pid: The process ID to find inodes for
        """
        inodes = collections.defaultdict(list)
        for fd in os.listdir("/proc/%s/fd" % pid):
            try:
                inode = os.readlink("/proc/%s/fd/%s" % (pid, fd))
            except OSError:
                # TODO: need comment here
                continue
            else:
                if inode.startswith('socket:['):
                    # the process is using a socket
                    inode = inode[8:][:-1]
                    inodes[inode].append((pid, int(fd)))
        return inodes

    def get_all_inodes(self):
        """Gets all inodes for all processes."""
        inodes = collections.defaultdict(list)
        for pid in pids():
            try:
                inodes.update(self.get_proc_inodes(pid))
            except OSError:
                # os.listdir() is gonna raise a lot of access denied
                # exceptions in case of unprivileged user; that's fine
                # as we'll just end up returning a connection with PID
                # and fd set to None anyway.
                # Both netstat -an and lsof does the same so it's
                # unlikely we can do any better.
                # ENOENT just means a PID disappeared on us.
                err = sys.exc_info()[1]
                if err.errno not in (
                        errno.ENOENT, errno.ESRCH, errno.EPERM, errno.EACCES):
                    raise
        return inodes

    @staticmethod
    def decode_address(addr, family):
        """Accept an "ip:port" address as displayed in /proc/net/*
        and convert it into a human readable form, like:

        "0500000A:0016" -> ("10.0.0.5", 22)
        "0000000000000000FFFF00000100007F:9E49" -> ("::ffff:127.0.0.1", 40521)

        The IP address portion is a little or big endian four-byte
        hexadecimal number; that is, the least significant byte is listed
        first, so we need to reverse the order of the bytes to convert it
        to an IP address.
        The port is represented as a two-byte hexadecimal number.

        Reference:
        http://linuxdevcenter.com/pub/a/linux/2000/11/16/LinuxAdmin.html
        """
        ip, port = addr.split(':')
        port = int(port, 16)
        # this usually refers to a local socket in listen mode with
        # no end-points connected
        if not port:
            return ()
        ip = ip.encode('ascii')
        if family == socket.AF_INET:
            # see: https://github.com/giampaolo/psutil/issues/201
            if sys.byteorder == 'little':
                ip = socket.inet_ntop(family, base64.b16decode(ip)[::-1])
            else:
                ip = socket.inet_ntop(family, base64.b16decode(ip))
        else:  # IPv6
            # old version - let's keep it, just in case...
            # ip = ip.decode('hex')
            # return socket.inet_ntop(socket.AF_INET6,
            # ''.join(ip[i:i+4][::-1] for i in xrange(0, 16, 4)))
            ip = base64.b16decode(ip)
            # see: https://github.com/giampaolo/psutil/issues/201
            if sys.byteorder == 'little':
                ip = socket.inet_ntop(
                    socket.AF_INET6,
                    struct.pack('>4I', *struct.unpack('<4I', ip)))
            else:
                ip = socket.inet_ntop(
                    socket.AF_INET6,
                    struct.pack('<4I', *struct.unpack('<4I', ip)))
        return (ip, port)

    def process_inet(self, path, family, type_, inodes, filter_pid=None): # pylint: disable=too-many-arguments
        """Parse /proc/net/tcp* and /proc/net/udp* files.

        :param path: path of the file to process
        :param family: one of socket.AF_INET, socket.AF_INET6, socket.AF_UNIX
        :param type_: socket.SOCK_STREAM, socket.SOCK_DGRAM, None
        :param inodes: the dictionary output from ``get_*_inodes()``
        :param filter_pid: A process ID to filter output
        """
        if path.endswith('6') and not os.path.exists(path):
            # IPv6 not supported
            return
        file = open(path, 'r')
        file.readline()  # skip the first line
        for line in file:
            _, laddr, raddr, status, _, _, _, _, _, inode = \
                line.split()[:10]
            if inode in inodes:
                # There may be multiple entries for this inode,
                # if there are multiple FDs for it. This used to
                # raise an exception, but now doesn't. See
                # also: https://github.com/giampaolo/psutil/issues/572
                pid, fd = inodes[inode][0]
            else:
                pid, fd = None, -1
            if filter_pid is not None and filter_pid != pid:
                continue
            else:
                if type_ == socket.SOCK_STREAM:
                    status = TCP_STATUSES[status]
                else:
                    status = 'NONE'
                laddr = self.decode_address(laddr, family)
                raddr = self.decode_address(raddr, family)
                yield (fd, family, type_, laddr, raddr, status, pid)
        file.close()

    @staticmethod
    def process_unix(path, family, inodes, filter_pid=None):
        """Parse /proc/net/unix files.

        :param path: path of the file to process
        :param family: one of socket.AF_INET, socket.AF_INET6, socket.AF_UNIX
        :param inodes: the dictionary output from ``self.get_*_inodes()``
        :param filter_pid: Optional. Only return sockets owned by this process
        """
        file = open(path, 'r')
        file.readline()  # skip the first line
        for line in file:
            tokens = line.split()
            _, _, _, _, type_, _, inode = tokens[0:7]
            if inode in inodes:
                # With UNIX sockets we can have a single inode
                # referencing many file descriptors.
                pairs = inodes[inode]
            else:
                pairs = [(None, -1)]
            for pid, fd in pairs:
                if filter_pid is not None and filter_pid != pid:
                    continue
                else:
                    if len(tokens) == 8:
                        path = tokens[-1]
                    else:
                        path = ""
                    type_ = int(type_)
                    raddr = None
                    status = 'NONE'
                    yield (fd, family, type_, path, raddr, status, pid)
        file.close()

    def retrieve(self, kind, pid=None):
        """Fetch connections.

        :param kind: The type of socket: socket.AF_*
        :param pid: Optional. Only return sockets owned by this process
        """
        if kind not in self.tmap:
            raise ValueError("invalid %r kind argument; choose between %s"
                             % (kind, ', '.join([repr(x) for x in self.tmap])))
        if pid is not None:
            inodes = self.get_proc_inodes(pid)
            if not inodes:
                # no connections for this process
                return []
        else:
            inodes = self.get_all_inodes()
        ret = []
        for fname, family, type_ in self.tmap[kind]:
            if family in (socket.AF_INET, socket.AF_INET6):
                socks = self.process_inet("/proc/net/%s" % fname, family,
                                          type_, inodes, filter_pid=pid)
            else:
                socks = self.process_unix(
                    "/proc/net/%s" % fname, family, inodes, filter_pid=pid)
            for fd, family, type_, laddr, raddr, status, bound_pid in socks:
                if pid:
                    conn = Connection(fd, family, type_, laddr, raddr,
                                      status, bound_pid)
                else:
                    conn = Connection(fd, family, type_, laddr, raddr,
                                      status, bound_pid)
                ret.append(conn)
        return ret
