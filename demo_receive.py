"""Minimal end-to-end receive demo: real loopback packets -> delivery events.

This keeps the original ``demo.py`` untouched.  A :class:`~forwarding.Forwarder`
sends encoded synthetic slices to two temporary IPv4 loopback UDP sockets; only
the datagrams actually received on those sockets are handed to a
:class:`~receiving.WindowReceiver`, which reassembles them and emits ordered
``message``/``gap`` events.  Nothing is sent until a socket really receives it,
and the receiver itself never touches the network.
"""

import socket

from forwarding import Forwarder
from receiving import WindowReceiver, encode_slice


def _loopback_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(2.0)
    return sock


def run():
    east_sock = _loopback_socket()
    west_sock = _loopback_socket()
    observed = []
    with east_sock, west_sock, Forwarder(
        {"east": east_sock.getsockname(), "west": west_sock.getsockname()}
    ) as router:
        router.replace([("192.0.2.0/24", "east"), ("198.51.100.0/24", "west")])
        receiver = WindowReceiver(
            {"east": (5, 3), "west": (4,)}, byte_budget=1024
        )

        # east sample 2 lands complete while sample 1 is still incomplete.
        router.send("192.0.2.10", encode_slice(2, 0, b"XYZ"))
        raw = east_sock.recv(4096)
        status, events = receiver.feed("east", raw)
        observed.append(("east", "sample-2-first", status, events,
                         receiver.buffered_bytes))

        router.send("192.0.2.10", encode_slice(1, 2, b"cde"))
        status, events = receiver.feed("east", east_sock.recv(4096))
        observed.append(("east", "sample-1-tail", status, events,
                         receiver.buffered_bytes))

        # A west slice interleaves; the two streams never share bytes.
        router.send("198.51.100.10", encode_slice(1, 0, b"west"))
        status, events = receiver.feed("west", west_sock.recv(4096))
        observed.append(("west", "complete", status, events,
                         receiver.buffered_bytes))

        # Completing east sample 1 releases both east samples in number order.
        router.send("192.0.2.10", encode_slice(1, 0, b"ab"))
        status, events = receiver.feed("east", east_sock.recv(4096))
        observed.append(("east", "sample-1-head", status, events,
                         receiver.buffered_bytes))

    for name, step, status, events, buffered in observed:
        print(f"{name:4s} {step:15s} status={status:9s} events={events} "
              f"buffered={buffered}")
    return observed


if __name__ == "__main__":
    run()
