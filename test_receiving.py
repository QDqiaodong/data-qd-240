"""Acceptance tests for receive-side reassembly and ordered delivery.

Expected bytes are always synthetic literals; the code under test is never used
to compute the expected sample contents.  Scenario 3 drives the real
``Forwarder.send`` path over temporary IPv4 loopback UDP sockets and feeds the
actually received raw datagrams into the receiver.
"""

import socket
import struct
import unittest

from forwarding import Forwarder
from receiving import WindowReceiver, encode_slice


def free_loopback_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(2.0)
    return sock


class EncodingTest(unittest.TestCase):
    def test_encode_layout_and_limits(self):
        datagram = encode_slice(7, 12, b"abc")
        self.assertEqual(datagram[:8], struct.pack(">II", 7, 12))
        self.assertEqual(datagram[8:], b"abc")

        for bad_number in (0, 2 ** 32, -1, True, "1", 1.0):
            with self.assertRaises(ValueError):
                encode_slice(bad_number, 0, b"a")
        for bad_offset in (-1, 2 ** 32, False, 0.0):
            with self.assertRaises(ValueError):
                encode_slice(1, bad_offset, b"a")
        for bad_data in (b"", "a", bytearray(b"a"), None):
            with self.assertRaises(ValueError):
                encode_slice(1, 0, bad_data)
        with self.assertRaises(ValueError):
            encode_slice(1, 0, b"x" * 4089)  # 8 + 4089 > 4096
        with self.assertRaises(ValueError):
            encode_slice(1, 65535, b"xy")  # offset + length > 65536
        # Boundary values are legal.
        self.assertEqual(len(encode_slice(1, 0, b"x" * 4088)), 4096)
        self.assertEqual(
            len(encode_slice(1, 65535, b"x")), 9
        )


class ConstructionTest(unittest.TestCase):
    def test_manifest_and_budget_validation(self):
        WindowReceiver({"s": (1,)}, 0)  # minimal legal instance
        with self.assertRaises(ValueError):
            WindowReceiver({}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"s": ()}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"s": (0,)}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"s": (65537,)}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"s": (True,)}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({1: (1,)}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"": (1,)}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"s": "ab"}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({"s": (1,) * 129}, 8)
        with self.assertRaises(ValueError):
            WindowReceiver({str(i): (1,) for i in range(9)}, 8)
        for bad_budget in (-1, 1_048_577, True, False, "8", 1.0):
            with self.assertRaises(ValueError):
                WindowReceiver({"s": (1,)}, bad_budget)


class FeedValidationTest(unittest.TestCase):
    def setUp(self):
        self.recv = WindowReceiver({"s": (6, 3)}, 1024)

    def test_unknown_stream_and_bad_datagrams(self):
        with self.assertRaises(ValueError):
            self.recv.feed("other", encode_slice(1, 0, b"a"))
        for bad in (
            b"short",
            b"",
            123,
            bytearray(b"x" * 9),
            struct.pack(">II", 0, 0) + b"a",  # number zero on the wire
        ):
            with self.assertRaises(ValueError):
                self.recv.feed("s", bad)
        with self.assertRaises(ValueError):
            self.recv.feed("s", encode_slice(3, 0, b"a"))
        with self.assertRaises(ValueError):
            self.recv.feed("s", encode_slice(1, 5, b"xy"))
        self.assertEqual(self.recv.buffered_bytes, 0)

    def test_late_never_reopens_terminated_number(self):
        status, events = self.recv.feed("s", encode_slice(1, 0, b"abcdef"))
        self.assertEqual(status, "accepted")
        self.assertEqual(events, (("message", "s", 1, b"abcdef"),))
        status, events = self.recv.feed("s", encode_slice(1, 0, b"abcdef"))
        self.assertEqual((status, events), ("late", ()))


class OverlapConflictTest(unittest.TestCase):
    def setUp(self):
        self.recv = WindowReceiver({"s": (6,)}, 1024)

    def test_duplicate_conflict_and_consistent_overlap(self):
        status, _ = self.recv.feed("s", encode_slice(1, 1, b"bc"))
        self.assertEqual(status, "accepted")
        self.assertEqual(self.recv.buffered_bytes, 2)

        # Exact retransmission adds no coverage.
        status, events = self.recv.feed("s", encode_slice(1, 1, b"bc"))
        self.assertEqual((status, events), ("duplicate", ()))
        self.assertEqual(self.recv.buffered_bytes, 2)

        # Consistent overlap that also extends coverage is accepted.
        status, _ = self.recv.feed("s", encode_slice(1, 2, b"cd"))
        self.assertEqual(status, "accepted")
        self.assertEqual(self.recv.buffered_bytes, 3)

        # Conflicting overlap: the whole datagram, including its brand-new
        # byte at position 4, must be rejected.
        status, events = self.recv.feed("s", encode_slice(1, 2, b"XZe"))
        self.assertEqual((status, events), ("conflict", ()))
        self.assertEqual(self.recv.buffered_bytes, 3)
        status, _ = self.recv.feed("s", encode_slice(1, 4, b"e"))
        self.assertEqual(status, "accepted")  # position 4 was not retained
        self.assertEqual(self.recv.buffered_bytes, 4)

    def test_conflict_leaves_new_stream_sample_untracked(self):
        recv = WindowReceiver({"s": (4, 4)}, 1024)
        # First datagram ever touches number 2; a conflict there must not leave
        # an empty sample shell behind.
        recv.feed("s", encode_slice(2, 0, b"ab"))
        status, _ = recv.feed("s", encode_slice(2, 0, b"ax"))
        self.assertEqual(status, "conflict")
        self.assertEqual(recv.buffered_bytes, 2)
        status, head_events = recv.feed("s", encode_slice(1, 0, b"qrst"))
        self.assertEqual(
            (status, head_events),
            ("accepted", (("message", "s", 1, b"qrst"),)),
        )
        # Number 2 still waits; its final slice then delivers it alone.
        status, events = recv.feed("s", encode_slice(2, 2, b"cd"))
        self.assertEqual(
            (status, events),
            ("accepted", (("message", "s", 2, b"abcd"),)),
        )
        self.assertEqual(recv.buffered_bytes, 0)


class ScenarioOneSealTest(unittest.TestCase):
    """Later sample complete first; decide whether to keep waiting for number 1."""

    def _build(self):
        # Two identical receivers get byte-identical inputs so the two
        # branches (no seal vs seal) are directly comparable.
        plain = WindowReceiver({"s": (6, 3)}, 1024)
        sealed = WindowReceiver({"s": (6, 3)}, 1024)
        for recv in (plain, sealed):
            status, events = recv.feed("s", encode_slice(1, 1, b"bcd"))
            self.assertEqual((status, events), ("accepted", ()))
            status, events = recv.feed("s", encode_slice(2, 0, b"XYZ"))
            self.assertEqual((status, events), ("accepted", ()))
            self.assertEqual(events, ())  # comparator still gets nothing
            self.assertEqual(recv.buffered_bytes, 6)
        return plain, sealed

    def test_without_seal_wait_then_deliver_in_order(self):
        recv, _ = self._build()
        # Fill the head and tail holes of sample 1 in two slices.
        status, events = recv.feed("s", encode_slice(1, 0, b"ab"))
        self.assertEqual((status, events), ("accepted", ()))
        status, events = recv.feed("s", encode_slice(1, 4, b"ef"))
        self.assertEqual(status, "accepted")
        self.assertEqual(
            events,
            (
                ("message", "s", 1, b"abcdef"),
                ("message", "s", 2, b"XYZ"),
            ),
        )
        self.assertEqual(recv.buffered_bytes, 0)
        print(
            "\n[scenario 1 observed] branch=no_seal "
            "completion_status=accepted; "
            "delivery_order=[message:1:b'abcdef', message:2:b'XYZ']; "
            "buffered_bytes=0",
            flush=True,
        )

    def test_seal_through_one_gap_then_late_slice(self):
        _, recv = self._build()
        self.assertEqual(recv.seal("s", 0), ())  # moving the bound from 0 ...
        events = recv.seal("s", 1)               # ... to 1
        self.assertEqual(
            events,
            (
                ("gap", "s", 1, "sealed"),
                ("message", "s", 2, b"XYZ"),
            ),
        )
        self.assertEqual(recv.buffered_bytes, 0)
        # The same completion slice must now be late; a gap never reverts.
        for datagram in (
            encode_slice(1, 0, b"ab"),
            encode_slice(1, 4, b"ef"),
            encode_slice(1, 1, b"bcd"),
        ):
            self.assertEqual(recv.feed("s", datagram), ("late", ()))
        self.assertEqual(recv.buffered_bytes, 0)
        print(
            "\n[scenario 1 observed] branch=seal_through_1 "
            "seal_events=[gap:1:sealed, message:2:b'XYZ']; "
            "later_slice_statuses=['late', 'late', 'late']; buffered_bytes=0",
            flush=True,
        )

    def test_conflict_overrun_and_budget_against_known_bytes(self):
        recv = WindowReceiver({"s": (6, 3)}, 1024)
        recv.feed("s", encode_slice(1, 1, b"bcd"))
        self.assertEqual(recv.buffered_bytes, 3)

        # Same-overlap slice with one conflicting byte and a new byte: the new
        # byte is lost together with the datagram.
        status, _ = recv.feed("s", encode_slice(1, 2, b"XZe"))
        self.assertEqual(status, "conflict")
        self.assertEqual(recv.buffered_bytes, 3)
        status, _ = recv.feed("s", encode_slice(1, 4, b"e"))
        self.assertEqual(status, "accepted")  # proves position 4 was not kept

        # Illegal slices raise and change nothing.
        with self.assertRaises(ValueError):
            recv.feed("s", encode_slice(1, 5, b"z!"))
        with self.assertRaises(ValueError):
            recv.feed("s", encode_slice(9, 0, b"z"))
        self.assertEqual(recv.buffered_bytes, 4)

        # A consistent duplicate stays a duplicate after the conflict.
        self.assertEqual(
            recv.feed("s", encode_slice(1, 1, b"bcd")), ("duplicate", ())
        )
        print(
            "\n[scenario 1 observed] overlap_check "
            "conflict_status=conflict (new byte discarded); "
            "overrun_statuses=[ValueError, ValueError]; "
            f"buffered_bytes={recv.buffered_bytes}",
            flush=True,
        )

    def test_never_received_number_becomes_gap_and_successor_flows(self):
        recv = WindowReceiver({"s": (3, 3, 3)}, 1024)
        events = recv.seal("s", 2)  # neither number ever saw a byte
        self.assertEqual(
            events,
            (
                ("gap", "s", 1, "sealed"),
                ("gap", "s", 2, "sealed"),
            ),
        )
        self.assertEqual(recv.feed("s", encode_slice(2, 0, b"abc")), ("late", ()))
        status, events = recv.feed("s", encode_slice(3, 0, b"XYZ"))
        self.assertEqual(
            (status, events),
            ("accepted", (("message", "s", 3, b"XYZ"),)),
        )
        self.assertEqual(recv.buffered_bytes, 0)

    def test_seal_validation_has_no_side_effects(self):
        recv = WindowReceiver({"s": (6, 3), "t": (2,)}, 1024)
        recv.feed("s", encode_slice(1, 0, b"ab"))
        for call in (("nope", 1), ("s", -1), ("s", 3), ("s", True), ("s", 1.0)):
            with self.assertRaises(ValueError):
                recv.seal(*call)
        self.assertEqual(recv.buffered_bytes, 2)
        # Partial sample sealed as a gap loses its bytes.
        events = recv.seal("s", 1)
        self.assertEqual(events, (("gap", "s", 1, "sealed"),))
        self.assertEqual(recv.buffered_bytes, 0)
        # Other streams are untouched.
        status, events = recv.feed("t", encode_slice(1, 0, b"hi"))
        self.assertEqual(
            (status, events), ("accepted", (("message", "t", 1, b"hi"),))
        )


class ScenarioTwoPressureTest(unittest.TestCase):
    """7 retained bytes, budget 8, an incoming slice needing 2 more."""

    POLICY = "retain_predecessor"

    def _primed(self):
        recv = WindowReceiver({"a": (6, 4, 2), "b": (4,)}, 8)
        self.assertEqual(recv.feed("a", encode_slice(1, 0, b"abc"))[0], "accepted")
        self.assertEqual(recv.feed("a", encode_slice(2, 0, b"WXYZ"))[0], "accepted")
        self.assertEqual(recv.buffered_bytes, 7)
        return recv

    def test_retain_predecessor_refuses_then_delivers_after_space_freed(self):
        recv = self._primed()

        # Stream b cannot borrow stream a's retained space.
        status, events = recv.feed("b", encode_slice(1, 0, b"01"))
        self.assertEqual((status, events), ("pressure", ()))
        self.assertEqual(recv.buffered_bytes, 7)
        # Stream a's coverage is intact.
        self.assertEqual(recv.feed("a", encode_slice(1, 0, b"abc"))[0], "duplicate")
        self.assertEqual(recv.feed("a", encode_slice(2, 0, b"WXYZ"))[0], "duplicate")

        # Completing the head sample frees both head samples in one call.
        status, events = recv.feed("a", encode_slice(1, 3, b"def"))
        self.assertEqual(status, "accepted")
        self.assertEqual(
            events,
            (
                ("message", "a", 1, b"abcdef"),
                ("message", "a", 2, b"WXYZ"),
            ),
        )
        self.assertEqual(recv.buffered_bytes, 0)

        # The rejected slice can be resubmitted locally once space exists.
        status, events = recv.feed("a", encode_slice(3, 0, b"PQ"))
        self.assertEqual(
            (status, events), ("accepted", (("message", "a", 3, b"PQ"),))
        )
        self.assertEqual(recv.buffered_bytes, 0)
        # Retrying an already delivered number reports late, never a gap.
        self.assertEqual(recv.feed("a", encode_slice(1, 3, b"def"))[0], "late")
        print(
            "\n[scenario 2 observed] policy=retain_predecessor "
            "pressure_status=pressure (stream b, retained=7, no cross-stream "
            "write-off); completion_status=accepted; "
            "delivery_order=[message:a:1, message:a:2]; "
            "resubmit_status=accepted [message:a:3]; final_buffered_bytes=0",
            flush=True,
        )

    def test_pressure_boundary_and_budget_zero(self):
        recv = self._primed()
        # One new byte exactly reaches the budget; a second cannot fit.
        self.assertEqual(recv.feed("b", encode_slice(1, 0, b"0"))[0], "accepted")
        self.assertEqual(recv.buffered_bytes, 8)
        self.assertEqual(
            recv.feed("b", encode_slice(1, 1, b"1")), ("pressure", ())
        )
        self.assertEqual(recv.buffered_bytes, 8)

        zero = WindowReceiver({"s": (3,)}, 0)
        # A retained partial slice never fits a zero budget.
        self.assertEqual(
            zero.feed("s", encode_slice(1, 0, b"ab")), ("pressure", ())
        )
        # A slice that completes the front sample needs zero persistent space.
        status, events = zero.feed("s", encode_slice(1, 0, b"abc"))
        self.assertEqual(
            (status, events), ("accepted", (("message", "s", 1, b"abc"),))
        )
        self.assertEqual(zero.buffered_bytes, 0)


class ReorderingAndDeliveryTest(unittest.TestCase):
    def test_many_samples_delivered_strictly_in_order_one_call_chain(self):
        recv = WindowReceiver({"s": (2, 2, 2)}, 1024)
        recv.feed("s", encode_slice(3, 0, b"gh"))  # farthest first
        recv.feed("s", encode_slice(2, 0, b"ef"))
        self.assertEqual(recv.buffered_bytes, 4)
        status, events = recv.feed("s", encode_slice(1, 0, b"cd"))
        self.assertEqual(status, "accepted")
        self.assertEqual(
            events,
            (
                ("message", "s", 1, b"cd"),
                ("message", "s", 2, b"ef"),
                ("message", "s", 3, b"gh"),
            ),
        )
        self.assertEqual(recv.buffered_bytes, 0)

    def test_streams_never_share_bytes_or_delivery_state(self):
        recv = WindowReceiver({"a": (4,), "b": (4,)}, 1024)
        recv.feed("a", encode_slice(1, 2, b"gh"))  # continuation on a
        recv.feed("b", encode_slice(1, 0, b"01"))  # first slice on b
        self.assertEqual(recv.buffered_bytes, 4)
        # b completes first; per-stream ordering lets b deliver while a waits.
        status, events = recv.feed("b", encode_slice(1, 2, b"23"))
        self.assertEqual(
            (status, events), ("accepted", (("message", "b", 1, b"0123"),))
        )
        self.assertEqual(recv.buffered_bytes, 2)
        status, events = recv.feed("a", encode_slice(1, 0, b"ef"))
        self.assertEqual(
            (status, events), ("accepted", (("message", "a", 1, b"efgh"),))
        )
        self.assertEqual(recv.buffered_bytes, 0)


class ScenarioThreeLoopbackTest(unittest.TestCase):
    """Real Forwarder.send over loopback; only received datagrams are fed."""

    def test_actual_packets_reassembled_by_receiving_socket(self):
        sock_a = free_loopback_socket()
        sock_b = free_loopback_socket()
        self.addCleanup(sock_a.close)
        self.addCleanup(sock_b.close)
        with Forwarder(
            {"a": sock_a.getsockname(), "b": sock_b.getsockname()}
        ) as router:
            router.replace(
                [("192.0.2.0/24", "a"), ("198.51.100.0/24", "b")]
            )
            recv = WindowReceiver({"a": (4, 4), "b": (4,)}, 1024)

            def send_and_collect(destination, datagram, sock, name):
                outcome = router.send(destination, datagram)
                raw = sock.recv(4096)  # block until the packet really arrives
                self.assertEqual(raw, datagram)
                status, events = recv.feed(name, raw)
                return outcome, status, events

            # Sample 2 of stream a arrives complete before sample 1; stream b's
            # first slice interleaves.  Nothing may be delivered yet.
            outcome, status, events = send_and_collect(
                "192.0.2.10", encode_slice(2, 0, b"SECD"), sock_a, "a"
            )
            self.assertEqual(outcome.egress, "a")
            self.assertEqual((status, events), ("accepted", ()))
            _, status, events = send_and_collect(
                "192.0.2.10", encode_slice(1, 2, b"gh"), sock_a, "a"
            )
            self.assertEqual((status, events), ("accepted", ()))
            _, status, events = send_and_collect(
                "198.51.100.10", encode_slice(1, 0, b"01"), sock_b, "b"
            )
            self.assertEqual((status, events), ("accepted", ()))
            self.assertEqual(recv.buffered_bytes, 8)

            # b finishes and is delivered while a is still missing its head.
            _, status, events = send_and_collect(
                "198.51.100.10", encode_slice(1, 2, b"23"), sock_b, "b"
            )
            self.assertEqual(
                (status, events), ("accepted", (("message", "b", 1, b"0123"),))
            )

            # a's missing head releases both a samples in number order.
            _, status, events = send_and_collect(
                "192.0.2.10", encode_slice(1, 0, b"ef"), sock_a, "a"
            )
            self.assertEqual(status, "accepted")
            self.assertEqual(
                events,
                (
                    ("message", "a", 1, b"efgh"),
                    ("message", "a", 2, b"SECD"),
                ),
            )
            self.assertEqual(recv.buffered_bytes, 0)

            # Both sockets must be empty: no extra datagrams were invented.
            for sock in (sock_a, sock_b):
                sock.setblocking(False)
                with self.assertRaises(BlockingIOError):
                    sock.recv(4096)
            print(
                "\n[scenario 3 observed] real_loopback "
                "b_delivery=[message:b:1:b'0123']; "
                "a_delivery=[message:a:1:b'efgh', message:a:2:b'SECD']; "
                "buffered_bytes=0; sockets_empty=True",
                flush=True,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
