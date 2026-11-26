"""Batch reservation, route protection, and concurrency acceptance tests.

The positive cases use real temporary IPv4 loopback UDP sockets and make their
own receiver-based expectations.  Concurrency cases use a send seam that stops
at the real-send boundary; events and bounded thread joins replace timing
guesses.
"""

from contextlib import contextmanager
import select
import socket
import threading
import time
import unittest

from forwarding import (
    BatchBusy,
    BatchRejected,
    CapacityError,
    Forwarder,
    Outcome,
    RouteProtected,
)


EAST = "203.0.113.10"
WEST_DEST = "198.51.100.20"
UNROUTED = "198.51.100.99"
DROP_DEST = "192.0.2.2"
VALID_DEST = "192.0.2.1"


@contextmanager
def udp_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(2.0)
    try:
        yield sock
    finally:
        sock.close()


def recv_datagram(sock):
    """Receive one expected datagram using the socket's bounded timeout."""
    return sock.recv(65535)


def assert_socket_empty(test_case, sock):
    """Assert immediately that loopback UDP has no already-queued datagram."""
    ready, _, _ = select.select([sock], [], [], 0)
    if ready:
        data = sock.recv(65535)
        test_case.fail(f"unexpected extra UDP datagram: {data!r}")


def egresses_for(east, west=None, **extra):
    values = {"east": east.getsockname()}
    if west is not None:
        values["west"] = west.getsockname()
    values.update(extra)
    return values


class BlockingSendForwarder(Forwarder):
    """Block one payload at the send seam, while optionally really sending."""

    def __init__(self, egresses, block_payload):
        super().__init__(egresses)
        self.block_payload = block_payload
        self.sent_payloads = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self._log_lock = threading.Lock()

    def _send_datagram(self, payload, address):
        with self._log_lock:
            self.sent_payloads.append(bytes(payload))
        if payload == self.block_payload:
            self.entered.set()
            if not self.release.wait(2.0):
                raise TimeoutError("blocked send boundary was not released")
        return super()._send_datagram(payload, address)


class ScriptedSendForwarder(Forwarder):
    """Really send the first call and raise a stable OSError on the second."""

    def __init__(self, egresses):
        super().__init__(egresses)
        self.sent_payloads = []
        self.second_error = OSError("scripted second send failure")

    def _send_datagram(self, payload, address):
        self.sent_payloads.append(bytes(payload))
        if len(self.sent_payloads) == 2:
            raise self.second_error
        return super()._send_datagram(payload, address)


class CommitBarrierForwarder(Forwarder):
    """Stop after reservation registration while still holding the state lock."""

    def __init__(self, egresses):
        super().__init__(egresses)
        self.committed = threading.Event()
        self.allow_commit_to_return = threading.Event()

    def _reservation_committed_locked(self, ticket, state):
        self.committed.set()
        if not self.allow_commit_to_return.wait(2.0):
            raise TimeoutError("reservation commit barrier was not released")


class ScenarioOnePublicationTest(unittest.TestCase):
    def test_same_candidate_with_and_without_ticket(self):
        observed_egresses = []
        with udp_receiver() as east, udp_receiver() as west:
            egresses = egresses_for(
                east,
                west,
                east_alias=east.getsockname(),
            )
            with Forwarder(egresses) as fw:
                initial = [
                    ("0.0.0.0/0", "west"),
                    ("203.0.113.0/24", "east"),
                ]
                fw.replace(initial)
                self.assertEqual(fw.revision, 1)

                ticket = fw.reserve([(EAST, b"s1-managed-east")])
                self.assertEqual(fw.active_sample_count, 1)
                assert_socket_empty(self, east)
                assert_socket_empty(self, west)

                west_candidate = [
                    ("0.0.0.0/0", "west"),
                    ("203.0.113.0/24", "west"),
                ]
                with self.assertRaises(RouteProtected) as ctx:
                    fw.replace(west_candidate)
                protected = ctx.exception
                self.assertEqual(protected.reason, "route_protected")
                self.assertEqual(protected.revision, 1)
                self.assertEqual(fw.revision, 1)
                self.assertEqual(
                    protected.conflicts[0]["promised_egress"], "east"
                )
                self.assertEqual(
                    protected.conflicts[0]["candidate_egress"], "west"
                )

                direct = fw.send(EAST, b"s1-direct-east")
                self.assertEqual(
                    direct, Outcome("sent", "203.0.113.0/24", "east")
                )
                direct_data = recv_datagram(east)
                observed_egresses.append(("east", direct_data.decode()))
                self.assertEqual(direct_data, b"s1-direct-east")

                managed = fw.dispatch_next(ticket)
                self.assertEqual(
                    managed, Outcome("sent", "203.0.113.0/24", "east")
                )
                managed_data = recv_datagram(east)
                observed_egresses.append(("east", managed_data.decode()))
                self.assertEqual(managed_data, b"s1-managed-east")
                assert_socket_empty(self, west)
                assert_socket_empty(self, east)

                with self.assertRaises(LookupError):
                    fw.dispatch_next(ticket)
                self.assertEqual(fw.revision, 1)

                # The identical candidate now publishes because the ticket ended.
                fw.replace(west_candidate)
                self.assertEqual(fw.revision, 2)
                west_direct = fw.send(EAST, b"s1-direct-west")
                self.assertEqual(
                    west_direct, Outcome("sent", "203.0.113.0/24", "west")
                )
                west_data = recv_datagram(west)
                observed_egresses.append(("west", west_data.decode()))
                self.assertEqual(west_data, b"s1-direct-west")
                assert_socket_empty(self, east)
                assert_socket_empty(self, west)

                # Reserve under /24/east, then publish a more specific /25/east.
                east_24 = [
                    ("0.0.0.0/0", "west"),
                    ("203.0.113.0/24", "east"),
                ]
                fw.replace(east_24)
                self.assertEqual(fw.revision, 3)
                old_prefix_ticket = fw.reserve(
                    [(EAST, b"s1-accepted-24-prefix")]
                )

                finer_east = [
                    ("0.0.0.0/0", "west"),
                    ("203.0.113.0/24", "west"),
                    ("203.0.113.0/25", "east"),
                ]
                fw.replace(finer_east)
                self.assertEqual(fw.revision, 4)

                finer_direct = fw.send(EAST, b"s1-direct-25-prefix")
                self.assertEqual(
                    finer_direct,
                    Outcome("sent", "203.0.113.0/25", "east"),
                )
                finer_data = recv_datagram(east)
                observed_egresses.append(("east", finer_data.decode()))

                old_managed = fw.dispatch_next(old_prefix_ticket)
                self.assertEqual(
                    old_managed,
                    Outcome("sent", "203.0.113.0/24", "east"),
                )
                old_data = recv_datagram(east)
                observed_egresses.append(("east", old_data.decode()))
                self.assertEqual(old_data, b"s1-accepted-24-prefix")
                assert_socket_empty(self, east)

                # An alias to the same loopback address is still another name.
                alias_ticket = fw.reserve([(EAST, b"s1-alias-promise")])
                alias_candidate = [
                    ("0.0.0.0/0", "west"),
                    ("203.0.113.0/24", "west"),
                    ("203.0.113.0/25", "east_alias"),
                ]
                with self.assertRaises(RouteProtected) as alias_ctx:
                    fw.replace(alias_candidate)
                self.assertEqual(fw.revision, 4)
                self.assertEqual(
                    alias_ctx.exception.conflicts[0]["promised_egress"],
                    "east",
                )
                self.assertEqual(
                    alias_ctx.exception.conflicts[0]["candidate_egress"],
                    "east_alias",
                )

                alias_managed = fw.dispatch_next(alias_ticket)
                self.assertEqual(
                    alias_managed,
                    Outcome("sent", "203.0.113.0/25", "east"),
                )
                alias_managed_data = recv_datagram(east)
                observed_egresses.append(
                    ("east(same socket, promised east)",
                     alias_managed_data.decode())
                )

                fw.replace(alias_candidate)
                self.assertEqual(fw.revision, 5)
                alias_outcome = fw.send(EAST, b"s1-named-alias-direct")
                self.assertEqual(
                    alias_outcome,
                    Outcome("sent", "203.0.113.0/25", "east_alias"),
                )
                alias_direct_data = recv_datagram(east)
                observed_egresses.append(
                    ("east(same socket, named east_alias)",
                     alias_direct_data.decode())
                )
                assert_socket_empty(self, west)
                assert_socket_empty(self, east)

        print(
            "\n[scenario 1 observed] "
            f"first rejection={type(protected).__name__}"
            f"(reason={protected.reason!r}, revision={protected.revision}); "
            f"alias rejection={type(alias_ctx.exception).__name__}"
            f"(candidate={alias_ctx.exception.conflicts[0]['candidate_egress']!r}); "
            f"final_revision={fw.revision}; "
            f"received_by_egress={observed_egresses}",
            flush=True,
        )


class ScenarioTwoConcurrencyTest(unittest.TestCase):
    def test_inflight_send_busy_independent_ticket_and_immediate_protection(self):
        block_payload = b"s2-blocking-east"
        observed = []
        with udp_receiver() as east, udp_receiver() as west:
            fw = BlockingSendForwarder(
                egresses_for(east, west), block_payload
            )
            with fw:
                initial = [
                    ("203.0.113.10/32", "east"),
                    ("198.51.100.20/32", "west"),
                    ("0.0.0.0/0", "west"),
                ]
                fw.replace(initial)
                self.assertEqual(fw.revision, 1)

                east_ticket = fw.reserve([(EAST, block_payload)])
                west_ticket = fw.reserve(
                    [(WEST_DEST, b"s2-independent-west")]
                )

                worker_result = {}

                def worker():
                    try:
                        worker_result["outcome"] = fw.dispatch_next(east_ticket)
                    except BaseException as exc:  # reported after join
                        worker_result["error"] = exc

                thread = threading.Thread(target=worker, daemon=True)
                thread.start()
                try:
                    self.assertTrue(
                        fw.entered.wait(2.0),
                        "blocked send did not reach its boundary",
                    )

                    with self.assertRaises(BatchBusy) as busy_dispatch:
                        fw.dispatch_next(east_ticket)
                    with self.assertRaises(BatchBusy) as busy_cancel:
                        fw.cancel(east_ticket)
                    self.assertEqual(
                        busy_dispatch.exception.reason, "batch_busy"
                    )
                    self.assertEqual(
                        busy_cancel.exception.reason, "batch_busy"
                    )
                    self.assertEqual(fw.sent_payloads.count(block_payload), 1)

                    west_outcome = fw.dispatch_next(west_ticket)
                    self.assertEqual(
                        west_outcome,
                        Outcome("sent", "198.51.100.20/32", "west"),
                    )
                    west_data = recv_datagram(west)
                    observed.append(("west-independent", west_data.decode()))
                    assert_socket_empty(self, west)

                    compatible = [
                        ("203.0.113.10/32", "east"),
                        ("198.51.100.20/32", "west"),
                        ("192.0.2.0/24", "west"),
                        ("0.0.0.0/0", "west"),
                    ]
                    fw.replace(compatible)
                    self.assertEqual(fw.revision, 2)

                    incompatible = [
                        ("203.0.113.10/32", "west"),
                        ("198.51.100.20/32", "west"),
                        ("192.0.2.0/24", "west"),
                        ("0.0.0.0/0", "west"),
                    ]
                    started = time.monotonic()
                    with self.assertRaises(RouteProtected) as protected_ctx:
                        fw.replace(incompatible)
                    immediate_elapsed = time.monotonic() - started
                    protected = protected_ctx.exception
                    self.assertLess(immediate_elapsed, 1.0)
                    self.assertEqual(protected.revision, 2)
                    self.assertEqual(fw.revision, 2)
                    self.assertTrue(
                        protected.conflicts[0]["in_flight"] is True
                    )

                    fw.release.set()
                    thread.join(2.0)
                    self.assertFalse(thread.is_alive())
                    self.assertNotIn("error", worker_result)
                    self.assertEqual(
                        worker_result["outcome"],
                        Outcome("sent", "203.0.113.10/32", "east"),
                    )

                    east_data = recv_datagram(east)
                    observed.append(("east-inflight", east_data.decode()))
                    self.assertEqual(east_data, block_payload)
                    self.assertEqual(
                        fw.sent_payloads.count(block_payload), 1
                    )

                    with self.assertRaises(LookupError):
                        fw.dispatch_next(east_ticket)

                    fw.replace(incompatible)
                    self.assertEqual(fw.revision, 3)
                    after = fw.send(EAST, b"s2-after-release-west")
                    self.assertEqual(
                        after, Outcome("sent", "203.0.113.10/32", "west")
                    )
                    after_data = recv_datagram(west)
                    observed.append(("west-after-release", after_data.decode()))
                    assert_socket_empty(self, east)
                    assert_socket_empty(self, west)
                finally:
                    fw.release.set()
                    thread.join(2.0)
                    self.assertFalse(thread.is_alive())

        print(
            "\n[scenario 2 observed] "
            f"concurrent_dispatch={type(busy_dispatch.exception).__name__}, "
            f"concurrent_cancel={type(busy_cancel.exception).__name__}; "
            f"inflight_rejection={type(protected).__name__}"
            f"(reason={protected.reason!r}, revision={protected.revision}, "
            f"immediate_elapsed_ms={immediate_elapsed * 1000:.1f}); "
            f"revision_after_retry={fw.revision}; "
            f"received_by_egress={observed}",
            flush=True,
        )

    def test_reserve_and_incompatible_replace_have_one_order(self):
        iterations = 100
        order_counts = {"reserve_first": 0, "replace_first": 0}
        with udp_receiver() as east, udp_receiver() as west:
            with Forwarder(egresses_for(east, west)) as fw:
                east_route = [("203.0.113.77/32", "east")]
                west_route = [("203.0.113.77/32", "west")]
                fw.replace(east_route)

                for i in range(iterations):
                    # Re-establish the same starting revision/table each round.
                    if fw.revision and i:
                        fw.replace(east_route)
                    start_revision = fw.revision
                    payload = f"order-{i}".encode()
                    barrier = threading.Barrier(2)
                    reserve_result = {}
                    replace_result = {}

                    def do_reserve():
                        barrier.wait(2.0)
                        try:
                            reserve_result["ticket"] = fw.reserve(
                                [("203.0.113.77", payload)]
                            )
                        except BaseException as exc:
                            reserve_result["error"] = exc

                    def do_replace():
                        barrier.wait(2.0)
                        try:
                            fw.replace(west_route)
                            replace_result["revision"] = fw.revision
                        except BaseException as exc:
                            replace_result["error"] = exc

                    reserve_thread = threading.Thread(
                        target=do_reserve, daemon=True
                    )
                    replace_thread = threading.Thread(
                        target=do_replace, daemon=True
                    )
                    reserve_thread.start()
                    replace_thread.start()
                    reserve_thread.join(2.0)
                    replace_thread.join(2.0)
                    self.assertFalse(reserve_thread.is_alive())
                    self.assertFalse(replace_thread.is_alive())
                    self.assertNotIn("error", reserve_result)
                    ticket = reserve_result["ticket"]

                    if "error" in replace_result:
                        self.assertIsInstance(
                            replace_result["error"], RouteProtected
                        )
                        order_counts["reserve_first"] += 1
                        self.assertEqual(fw.revision, start_revision)
                        outcome = fw.dispatch_next(ticket)
                        self.assertEqual(outcome.egress, "east")
                        data = recv_datagram(east)
                        self.assertEqual(data, payload)
                        # Once the promise ends, the same publication succeeds.
                        fw.replace(west_route)
                    else:
                        order_counts["replace_first"] += 1
                        self.assertEqual(
                            replace_result["revision"], start_revision + 1
                        )
                        self.assertEqual(fw.revision, start_revision + 1)
                        outcome = fw.dispatch_next(ticket)
                        self.assertEqual(outcome.egress, "west")
                        data = recv_datagram(west)
                        self.assertEqual(data, payload)

                    assert_socket_empty(self, east)
                    assert_socket_empty(self, west)

        print(
            "\n[reserve/replace ordering observed] "
            f"iterations={iterations}, counts={order_counts}, "
            f"final_revision={fw.revision}",
            flush=True,
        )

    def test_registered_old_promise_cannot_be_followed_by_new_table(self):
        observed = {}
        with udp_receiver() as east, udp_receiver() as west:
            fw = CommitBarrierForwarder(egresses_for(east, west))
            with fw:
                east_route = [("203.0.113.88/32", "east")]
                west_route = [("203.0.113.88/32", "west")]
                fw.replace(east_route)

                reserve_result = {}

                def reserve_worker():
                    reserve_result["ticket"] = fw.reserve(
                        [("203.0.113.88", b"commit-order")]
                    )

                reserve_thread = threading.Thread(
                    target=reserve_worker, daemon=True
                )
                reserve_thread.start()
                try:
                    self.assertTrue(fw.committed.wait(2.0))

                    replace_result = {}

                    def replace_worker():
                        try:
                            fw.replace(west_route)
                        except BaseException as exc:
                            replace_result["error"] = exc

                    replace_thread = threading.Thread(
                        target=replace_worker, daemon=True
                    )
                    replace_thread.start()

                    # Releasing the commit barrier orders this reservation
                    # before the waiting/next publication attempt.
                    fw.allow_commit_to_return.set()
                    reserve_thread.join(2.0)
                    replace_thread.join(2.0)
                    self.assertFalse(reserve_thread.is_alive())
                    self.assertFalse(replace_thread.is_alive())

                    self.assertNotIn("error", reserve_result)
                    self.assertIsInstance(
                        replace_result.get("error"), RouteProtected
                    )
                    protected = replace_result["error"]
                    self.assertEqual(protected.revision, 1)
                    self.assertEqual(fw.revision, 1)

                    outcome = fw.dispatch_next(reserve_result["ticket"])
                    self.assertEqual(
                        outcome,
                        Outcome("sent", "203.0.113.88/32", "east"),
                    )
                    data = recv_datagram(east)
                    observed["egress"] = "east"
                    observed["payload"] = data.decode()

                    fw.replace(west_route)
                    self.assertEqual(fw.revision, 2)
                finally:
                    fw.allow_commit_to_return.set()
                    reserve_thread.join(2.0)
                    if "replace_thread" in locals():
                        replace_thread.join(2.0)

        print(
            "\n[deterministic reserve-first observed] "
            f"rejection={type(protected).__name__}"
            f"(revision={protected.revision}); "
            f"published_after_send_revision={fw.revision}; "
            f"dispatch={outcome!r}; received={observed}",
            flush=True,
        )


class RejectionCapacityAndFailureTest(unittest.TestCase):
    def test_invalid_and_unroutable_batches_reject_position(self):
        with udp_receiver() as east:
            with Forwarder(egresses_for(east)) as fw:
                fw.replace([
                    ("192.0.2.1/32", "east"),
                    ("192.0.2.2/32", None),
                ])

                bad_batches = [
                    (
                        "unrouted",
                        [(VALID_DEST, b"good"), (UNROUTED, b"no-route")],
                        1,
                        "unrouted",
                    ),
                    (
                        "drop",
                        [(VALID_DEST, b"good"), (DROP_DEST, b"dropped")],
                        1,
                        "drop",
                    ),
                    (
                        "invalid-address",
                        [(VALID_DEST, b"good"), ("not-ipv4", b"bad")],
                        1,
                        "invalid",
                    ),
                    (
                        "invalid-payload-type",
                        [(VALID_DEST, b"good"), (VALID_DEST, "bytes?")],
                        1,
                        "invalid",
                    ),
                    (
                        "oversized-payload",
                        [(VALID_DEST, b"good"),
                         (VALID_DEST, b"x" * 4097)],
                        1,
                        "invalid",
                    ),
                ]

                for label, batch, expected_index, expected_reason in bad_batches:
                    with self.subTest(label=label):
                        before = fw.revision
                        with self.assertRaises(BatchRejected) as ctx:
                            fw.reserve(batch)
                        self.assertIsInstance(ctx.exception, ValueError)
                        self.assertEqual(ctx.exception.index, expected_index)
                        self.assertEqual(ctx.exception.position, expected_index)
                        self.assertEqual(ctx.exception.reason, expected_reason)
                        self.assertEqual(fw.revision, before)
                        self.assertEqual(fw.active_sample_count, 0)
                        assert_socket_empty(self, east)

                with self.assertRaises(BatchRejected) as empty_ctx:
                    fw.reserve([])
                self.assertEqual(empty_ctx.exception.reason, "empty")
                self.assertIsNone(empty_ctx.exception.index)

                with self.assertRaises(CapacityError) as too_large_ctx:
                    fw.reserve([(VALID_DEST, b"x")] * 17)
                self.assertEqual(too_large_ctx.exception.reason, "too_large")
                self.assertEqual(fw.active_sample_count, 0)
                assert_socket_empty(self, east)

    def test_64_capacity_direct_send_and_release(self):
        with udp_receiver() as east:
            with Forwarder(egresses_for(east)) as fw:
                fw.replace([("0.0.0.0/0", "east")])
                tickets = []
                serial = 0
                for _ in range(4):
                    batch = []
                    for _ in range(16):
                        batch.append(("192.0.2.1", f"fill-{serial}".encode()))
                        serial += 1
                    tickets.append(fw.reserve(batch))
                self.assertEqual(fw.active_sample_count, 64)

                with self.assertRaises(CapacityError) as full_ctx:
                    fw.reserve([("192.0.2.9", b"overflow")])
                self.assertEqual(full_ctx.exception.reason, "capacity")
                self.assertEqual(full_ctx.exception.available, 0)
                self.assertEqual(fw.active_sample_count, 64)

                direct = fw.send("192.0.2.1", b"direct-bypasses-quota")
                self.assertEqual(direct.egress, "east")
                self.assertEqual(fw.active_sample_count, 64)
                self.assertEqual(recv_datagram(east), b"direct-bypasses-quota")

                first = fw.dispatch_next(tickets[0])
                self.assertEqual(first, Outcome("sent", "0.0.0.0/0", "east"))
                self.assertEqual(fw.active_sample_count, 63)
                self.assertEqual(recv_datagram(east), b"fill-0")

                # The successful dispatch made exactly one new slot usable.
                one_slot = fw.reserve([("192.0.2.7", b"one-slot")])
                self.assertEqual(fw.active_sample_count, 64)
                fw.cancel(one_slot)
                self.assertEqual(fw.active_sample_count, 63)

                # The other 15 entries on tickets[0] can be cancelled.
                self.assertEqual(fw.cancel(tickets[0]), 15)
                self.assertEqual(fw.active_sample_count, 48)

                replacement = fw.reserve([
                    ("192.0.2.9", f"replacement-{i}".encode())
                    for i in range(16)
                ])
                self.assertEqual(fw.active_sample_count, 64)

                # Cancelling a full ticket releases its whole 16-slot batch.
                self.assertEqual(fw.cancel(tickets[1]), 16)
                self.assertEqual(fw.active_sample_count, 48)
                another = fw.reserve([("192.0.2.10", b"one-more")])
                self.assertIsNotNone(another)
                self.assertEqual(fw.active_sample_count, 49)

                fw.cancel(replacement)
                fw.cancel(another)
                fw.cancel(tickets[2])
                fw.cancel(tickets[3])
                self.assertEqual(fw.active_sample_count, 0)
                assert_socket_empty(self, east)

    def test_cancel_returns_unsent_count_and_does_not_undo_send(self):
        with udp_receiver() as east:
            with Forwarder(egresses_for(east)) as fw:
                fw.replace([("0.0.0.0/0", "east")])
                ticket = fw.reserve([
                    ("192.0.2.1", b"one"),
                    ("192.0.2.1", b"two"),
                    ("192.0.2.1", b"three"),
                ])
                self.assertEqual(
                    fw.dispatch_next(ticket),
                    Outcome("sent", "0.0.0.0/0", "east"),
                )
                self.assertEqual(recv_datagram(east), b"one")
                self.assertEqual(fw.cancel(ticket), 2)
                with self.assertRaises(LookupError):
                    fw.cancel(ticket)
                assert_socket_empty(self, east)

    def test_busy_cancel_preserves_remainder_for_later_cancel(self):
        block = b"busy-first"
        with udp_receiver() as east:
            fw = BlockingSendForwarder(egresses_for(east), block)
            with fw:
                fw.replace([("0.0.0.0/0", "east")])
                ticket = fw.reserve([
                    ("192.0.2.1", block),
                    ("192.0.2.1", b"busy-second"),
                ])
                result = {}

                def worker():
                    result["outcome"] = fw.dispatch_next(ticket)

                thread = threading.Thread(target=worker, daemon=True)
                thread.start()
                try:
                    self.assertTrue(fw.entered.wait(2.0))
                    with self.assertRaises(BatchBusy):
                        fw.cancel(ticket)
                    fw.release.set()
                    thread.join(2.0)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(
                        result["outcome"],
                        Outcome("sent", "0.0.0.0/0", "east"),
                    )
                    self.assertEqual(recv_datagram(east), block)
                    self.assertEqual(fw.cancel(ticket), 1)
                finally:
                    fw.release.set()
                    thread.join(2.0)

    def test_oserror_ends_ticket_and_releases_remaining_protection(self):
        with udp_receiver() as east, udp_receiver() as west:
            fw = ScriptedSendForwarder(egresses_for(east, west))
            with fw:
                fw.replace([("0.0.0.0/0", "east")])
                ticket = fw.reserve([
                    ("192.0.2.1", b"ok-one"),
                    ("192.0.2.1", b"error-two"),
                    ("192.0.2.1", b"never-three"),
                ])
                west_candidate = [("0.0.0.0/0", "west")]

                with self.assertRaises(RouteProtected):
                    fw.replace(west_candidate)

                self.assertEqual(
                    fw.dispatch_next(ticket),
                    Outcome("sent", "0.0.0.0/0", "east"),
                )
                self.assertEqual(recv_datagram(east), b"ok-one")

                with self.assertRaises(RouteProtected):
                    fw.replace(west_candidate)

                with self.assertRaises(OSError) as err_ctx:
                    fw.dispatch_next(ticket)
                self.assertIs(err_ctx.exception, fw.second_error)
                self.assertEqual(
                    fw.sent_payloads, [b"ok-one", b"error-two"]
                )

                with self.assertRaises(LookupError):
                    fw.dispatch_next(ticket)
                with self.assertRaises(LookupError):
                    fw.cancel(ticket)
                self.assertEqual(fw.active_sample_count, 0)

                # Entries two and three are gone; the formerly conflicting
                # candidate can now publish.
                fw.replace(west_candidate)
                self.assertEqual(fw.revision, 2)
                assert_socket_empty(self, east)

    def test_invalid_replace_keeps_table_revision_and_ticket(self):
        with udp_receiver() as east:
            with Forwarder(egresses_for(east)) as fw:
                good = [("0.0.0.0/0", "east")]
                fw.replace(good)
                ticket = fw.reserve([("192.0.2.1", b"still-promised")])
                revision = fw.revision

                for bad in (
                    [("192.0.2.1/24", "east")],
                    [("0.0.0.0/0", "missing")],
                    [("192.0.2.0/24", None),
                     ("192.0.2.0/24", None)],
                ):
                    with self.subTest(bad=bad):
                        with self.assertRaises(ValueError):
                            fw.replace(iter(bad))
                        self.assertEqual(fw.revision, revision)

                outcome = fw.dispatch_next(ticket)
                self.assertEqual(
                    outcome, Outcome("sent", "0.0.0.0/0", "east")
                )
                self.assertEqual(recv_datagram(east), b"still-promised")

    def test_unknown_and_foreign_tickets_are_lookup_errors_without_send(self):
        with udp_receiver() as one, udp_receiver() as two:
            with Forwarder(egresses_for(one)) as fw:
                fw.replace([("0.0.0.0/0", "east")])
                sends = []
                fw._send_datagram = lambda payload, address: sends.append(payload)

                with Forwarder(egresses_for(two)) as other:
                    other.replace([("0.0.0.0/0", "east")])
                    foreign = other.reserve([("192.0.2.1", b"foreign")])

                for bogus in (object(), None, "not-a-ticket", [], {}):
                    with self.subTest(bogus=repr(bogus)):
                        with self.assertRaises(LookupError):
                            fw.dispatch_next(bogus)
                        with self.assertRaises(LookupError):
                            fw.cancel(bogus)

                with self.assertRaises(LookupError):
                    fw.dispatch_next(foreign)
                with self.assertRaises(LookupError):
                    fw.cancel(foreign)
                self.assertEqual(sends, [])

    def test_context_exit_abandons_ticket_and_closes_socket(self):
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver_socket.bind(("127.0.0.1", 0))
        try:
            fw = Forwarder(egresses_for(receiver_socket))
            fw.replace([("0.0.0.0/0", "east")])
            ticket = fw.reserve([("192.0.2.1", b"abandoned")])
            sock = fw._socket
            fw.__exit__(None, None, None)
            self.assertEqual(sock.fileno(), -1)
            with self.assertRaises(LookupError):
                fw.dispatch_next(ticket)
        finally:
            receiver_socket.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
