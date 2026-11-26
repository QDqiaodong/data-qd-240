"""Userspace IPv4 destination policy with explicit loopback UDP egress."""
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv4Network
import socket


@dataclass(frozen=True)
class Outcome:
    action: str
    prefix: str = None
    egress: str = None


class Forwarder:
    """Route test datagrams using a replaceable longest-prefix table."""

    def __init__(self, egresses):
        self._egresses = dict(egresses)
        if not self._egresses or any(
            type(name) is not str or not name or not isinstance(address, tuple)
            or len(address) != 2 or type(address[0]) is not str
            or not IPv4Address(address[0]).is_loopback
            or type(address[1]) is not int or not 1 <= address[1] <= 65535
            for name, address in self._egresses.items()
        ):
            raise ValueError("egresses must be named IPv4 loopback destinations")
        self._routes = ()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def replace(self, routes):
        """Set all (strict CIDR, egress name or None to drop) entries."""
        table = {}
        for prefix, egress in routes:
            if type(prefix) is not str:
                raise ValueError("prefix must be text")
            network = IPv4Network(prefix, strict=True)
            if network in table or (egress is not None
                                    and (type(egress) is not str or egress not in self._egresses)):
                raise ValueError("duplicate prefix or unknown egress")
            table[network] = egress
        self._routes = tuple(sorted(table.items(), key=lambda entry: -entry[0].prefixlen))

    def send(self, destination, payload):
        """Return the actual action; a send error propagates without fallback."""
        if type(destination) is not str:
            raise ValueError("destination must be IPv4 text")
        address = IPv4Address(destination)
        if not isinstance(payload, bytes) or len(payload) > 4096:
            raise ValueError("expected at most 4096 bytes")
        for network, egress in self._routes:
            if address in network:
                if egress is None:
                    return Outcome("drop", str(network))
                self._socket.sendto(payload, self._egresses[egress])
                return Outcome("sent", str(network), egress)
        return Outcome("unrouted")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._socket.close()
