"""Minimal end-to-end receive demo: real loopback packets -> delivery events.

This keeps the original ``demo.py`` untouched.  A :class:`~forwarding.Forwarder`
sends encoded synthetic slices to two temporary IPv4 loopback UDP sockets; only
the datagrams actually received on those sockets are handed to a
:class:`~receiving.WindowReceiver`, which reassembles them and emits ordered
``message``/``gap`` events.  Nothing is sent until a socket really receives it,
and the receiver itself never touches the network.

The demo can optionally record those non-empty event groups, exactly as the
receiver produced them, into a local :class:`~delivery_spool.DeliverySpool`::

    python3 demo_receive.py                       # unchanged default output
    python3 demo_receive.py --spool /tmp/drill    # record a first run
    python3 demo_receive.py --read /tmp/drill     # reopen and continue reading

``--spool PATH`` creates a fresh record at ``PATH`` (an existing path is
refused, never overwritten) and feeds it the groups the drill actually
produced.  ``--read PATH`` opens the record left behind by such a run, shows
the comparator resuming from its confirmed cursor, then confirms, reclaims and
re-reads.  Recording stays off unless one of those flags is given, so the
default output is byte-for-byte the original one.
"""

import argparse
import os
import socket
import tempfile

from delivery_spool import CapacityExceeded, DeliverySpool
from forwarding import Forwarder
from receiving import WindowReceiver, encode_slice

COMPARATOR = "comparator"
SPOOL_CAPACITY = 64


def _loopback_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(2.0)
    return sock


class _OptionalRecorder:
    """Record only non-empty groups the receiver actually produced.

    Numbering always follows ``next_group_number`` so a group is never skipped.
    If the record runs out of capacity the demo stops recording instead of
    submitting later groups under a shifted number; the original drill output
    is unaffected either way.
    """

    def __init__(self, spool):
        self.spool = spool
        self.enabled = spool is not None

    def record(self, events):
        if not self.enabled or not events:
            return None
        try:
            return self.spool.submit(self.spool.next_group_number, events)
        except CapacityExceeded as exc:
            self.enabled = False
            print(f"recording capacity exhausted, later groups not recorded: "
                  f"{exc}")
            return None


def _run_drill(router, east_sock, west_sock, recorder):
    """Send and receive the fixed synthetic drill; record produced groups."""
    receiver = WindowReceiver({"east": (5, 3), "west": (4,)}, byte_budget=1024)
    observed = []

    def step(name, label, destination, datagram, sock):
        router.send(destination, datagram)
        raw = sock.recv(4096)  # only what really arrived is reassembled
        status, events = receiver.feed(name, raw)
        observed.append((name, label, status, events, receiver.buffered_bytes))
        recorder.record(events)

    # east sample 2 lands complete while sample 1 is still incomplete.
    step("east", "sample-2-first", "192.0.2.10",
         encode_slice(2, 0, b"XYZ"), east_sock)
    step("east", "sample-1-tail", "192.0.2.10",
         encode_slice(1, 2, b"cde"), east_sock)
    # A west slice interleaves; the two streams never share bytes.
    step("west", "complete", "198.51.100.10",
         encode_slice(1, 0, b"west"), west_sock)
    # Completing east sample 1 releases both east samples in number order.
    step("east", "sample-1-head", "192.0.2.10",
         encode_slice(1, 0, b"ab"), east_sock)
    return observed


def run(spool_path=None):
    """Run the drill, optionally recording its produced groups.

    With ``spool_path`` the record is created first and closed when this
    function returns; use :func:`read_record` (or ``--read``) afterwards to
    reopen it in a separate call.
    """
    spool = None
    if spool_path is not None:
        spool = DeliverySpool.create(os.fspath(spool_path), [COMPARATOR],
                                     SPOOL_CAPACITY)
    east_sock = _loopback_socket()
    west_sock = _loopback_socket()
    with east_sock, west_sock, Forwarder(
        {"east": east_sock.getsockname(), "west": west_sock.getsockname()}
    ) as router:
        router.replace([("192.0.2.0/24", "east"),
                        ("198.51.100.0/24", "west")])
        observed = _run_drill(
            router, east_sock, west_sock, _OptionalRecorder(spool)
        )

    for name, label, status, events, buffered in observed:
        print(f"{name:4s} {label:15s} status={status:9s} events={events} "
              f"buffered={buffered}")
    if spool is not None:
        print(f"delivery record created: {spool.path} "
              f"(reader={COMPARATOR!r}, capacity={spool.capacity}, "
              f"groups={spool.committed_groups}, "
              f"highest_delivery={spool.highest_delivery})")
    return observed


def read_record(path, page_size=2):
    """Reopen a record as the comparator and demonstrate restartable reads."""
    spool = DeliverySpool.open(os.fspath(path))
    committed = spool.highest_delivery
    cursor = spool.cursor(COMPARATOR)
    delivery = spool.read(COMPARATOR, page_size)
    print(f"record reopened: groups={spool.committed_groups} "
          f"highest_delivery={committed} comparator_cursor={cursor} "
          f"usage={spool.usage} page={delivery}")
    if delivery:
        spool.confirm(COMPARATOR, delivery[-1][0])
        print(f"comparator confirmed through {spool.cursor(COMPARATOR)}")
        try:
            result = spool.reclaim()
        except Exception as exc:
            print(f"reclaim skipped: {exc}")
        else:
            print(f"record reclaimed through {result.reclaimed_through}, "
                  f"usage now {result.usage}")
        print(f"comparator reread after reclamation: "
              f"{spool.read(COMPARATOR, 10)}")
    return spool


def _default_spool_path():
    base = tempfile.mkdtemp(prefix="demo-delivery-")
    return os.path.join(base, "drill.spool")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spool",
        nargs="?",
        const=_default_spool_path(),
        default=None,
        metavar="PATH",
        help="create a record at PATH and record non-empty produced groups",
    )
    parser.add_argument(
        "--read",
        nargs="?",
        const="",
        default=None,
        metavar="PATH",
        help="reopen a recorded drill and show the comparator resuming",
    )
    args = parser.parse_args(argv)
    if args.spool is not None and args.read is not None:
        parser.error("--spool and --read are separate runs; give only one")
    if args.read == "":
        parser.error("--read requires the path of a previously recorded drill")
    if args.spool is not None:
        run(spool_path=args.spool)
    elif args.read is not None:
        read_record(args.read)
    else:
        run()


if __name__ == "__main__":
    main()
