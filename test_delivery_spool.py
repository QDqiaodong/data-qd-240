"""Acceptance tests for the restartable local delivery record.

The record is fed event tuples that a real :class:`~receiving.WindowReceiver`
produces (scenario 1) or literal synthetic event tuples written out here
(scenarios 2 and 3), so the expected delivery numbers, retained sizes and page
contents are all independent literals.  Persistence-failure fixtures patch the
real ``os.replace`` commit boundary and the real post-commit hook; they never
simulate a write failure through invalid arguments.
"""

import os
import unittest
import tempfile

import delivery_spool
from delivery_spool import (
    CapacityExceeded,
    CorruptRecord,
    DeliverySpool,
    EmptyGroup,
    GroupConflict,
    GroupGap,
    GroupNumberError,
    InvalidAck,
    InvalidEvent,
    InvalidPageSize,
    NothingToReclaim,
    RecordExists,
    SpoolWriteError,
    StaleGroup,
    UnknownReader,
)
from receiving import WindowReceiver, encode_slice

SCENARIO_ONE_GROUP = (
    ("gap", "s", 1, "sealed"),
    ("message", "s", 2, b"XYZ"),
)
SCENARIO_ONE_SECOND = (("message", "t", 1, b"WXYZ"),)


def _real_receiver_group_one():
    """Drive a real receiver: later sample first, then seal the gap."""
    recv = WindowReceiver({"s": (5, 3)}, 1024)
    status, events = recv.feed("s", encode_slice(2, 0, b"XYZ"))
    assert (status, events) == ("accepted", ())
    events = recv.seal("s", 1)
    assert events == SCENARIO_ONE_GROUP
    return events


def _real_receiver_group_two():
    recv = WindowReceiver({"t": (4,)}, 1024)
    status, events = recv.feed("t", encode_slice(1, 0, b"WXYZ"))
    assert status == "accepted"
    assert events == SCENARIO_ONE_SECOND
    return events


class _FailingReplace:
    """Stand-in for the real commit boundary that never commits."""

    def __init__(self):
        self.attempts = 0

    def __call__(self, source, destination):
        self.attempts += 1
        raise OSError("injected failure before the commit boundary")


class _ReceiptLost(Exception):
    """Raised from the real post-commit hook to model a lost receipt."""


class _TempRoot(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = os.path.join(self._tmp.name, "record.spool")

    def reopen(self):
        return DeliverySpool.open(self.path)


class ConfigurationTest(_TempRoot):
    def test_create_validation_and_no_overwrite(self):
        for readers in ((), ["a", "a"], [""], [1], "ab"):
            with self.assertRaises(ValueError):
                DeliverySpool.create(self.path, readers, 8)
        for capacity in (0, -1, True, 8.0, "8"):
            with self.assertRaises(ValueError):
                DeliverySpool.create(self.path, ["a"], capacity)
        spool = DeliverySpool.create(self.path, ["a"], 8)
        self.assertEqual(spool.readers, ("a",))
        self.assertEqual(spool.capacity, 8)
        self.assertEqual(spool.usage, 0)
        self.assertEqual(spool.highest_delivery, 0)
        with open(self.path, "rb") as handle:
            before = handle.read()
        with self.assertRaises(RecordExists):
            DeliverySpool.create(self.path, ["b"], 99)
        with open(self.path, "rb") as handle:
            self.assertEqual(handle.read(), before)

    def test_reopen_keeps_original_configuration(self):
        DeliverySpool.create(self.path, ("a", "b"), 64)
        spool = self.reopen()
        self.assertEqual(spool.readers, ("a", "b"))
        self.assertEqual(spool.capacity, 64)
        self.assertEqual(spool.next_group_number, 1)
        self.assertEqual(spool.cursor("b"), 0)

    def test_corrupt_record_is_rejected(self):
        DeliverySpool.create(self.path, ("a",), 8)
        with open(self.path, "rb") as handle:
            good = handle.read()
        for damaged in (b"not json", b"{}", good[:-1]):
            with open(self.path, "wb") as handle:
                handle.write(damaged)
            with self.assertRaises(CorruptRecord):
                DeliverySpool.open(self.path)

    def test_reader_and_argument_validation(self):
        spool = DeliverySpool.create(self.path, ["a"], 64)
        events = [("message", "s", 1, b"abc")]
        spool.submit(1, events)
        with self.assertRaises(UnknownReader):
            spool.read("ghost", 1)
        with self.assertRaises(UnknownReader):
            spool.confirm("ghost", 1)
        for bad_limit in (0, -1, True, 2.0, "2"):
            with self.assertRaises(InvalidPageSize):
                spool.read("a", bad_limit)
        for bad_ack in (-1, 2, True, 1.0, "1"):
            with self.assertRaises(InvalidAck):
                spool.confirm("a", bad_ack)
        with self.assertRaises(GroupGap):
            spool.submit(3, events)
        with self.assertRaises(GroupNumberError):
            spool.submit(0, events)  # group number zero is not positive
        for bad_events in ([], (), [("message", "s", 1)], [("x", "s", 1, b"a")],
                           [("message", "", 1, b"a")],
                           [("message", "s", 1, b"")],
                           [("gap", "s", 1, "")],
                           [("gap", "s", True, "sealed")],
                           [("message", "s", 1, "text")]):
            with self.assertRaises(InvalidEvent):
                spool.submit(2, bad_events)
        with self.assertRaises(EmptyGroup):
            spool.submit(2, [])
        # Nothing above consumed a group number or a delivery number.
        self.assertEqual(spool.next_group_number, 2)
        self.assertEqual(spool.highest_delivery, 1)
        self.assertEqual(spool.usage, 3)


class ScenarioOneRestartTest(_TempRoot):
    """Interrupted group write, lost receipt, and post-reclaim replay."""

    def test_interrupted_write_then_lost_receipt_then_reclaim(self):
        group_one = _real_receiver_group_one()
        group_two = _real_receiver_group_two()

        DeliverySpool.create(self.path, ["r1"], 16)
        listing_before = set(os.listdir(self._tmp.name))
        original_replace = delivery_spool._atomic_replace
        failing = _FailingReplace()
        delivery_spool._atomic_replace = failing
        try:
            spool = self.reopen()
            with self.assertRaises(SpoolWriteError):
                spool.submit(1, group_one)
        finally:
            delivery_spool._atomic_replace = original_replace
        self.assertEqual(failing.attempts, 1)
        # No half group is visible and no number was consumed.
        self.assertEqual(set(os.listdir(self._tmp.name)), listing_before)
        spool = self.reopen()
        self.assertEqual(spool.highest_delivery, 0)
        self.assertEqual(spool.next_group_number, 1)
        self.assertEqual(spool.read("r1", 10), [])
        self.assertEqual(spool.usage, 0)

        # Same group number retried after unpatching commits contiguously.
        receipt = spool.submit(1, group_one)
        self.assertEqual(
            (receipt.first_delivery, receipt.last_delivery), (1, 2)
        )
        page = spool.read("r1", 10)
        self.assertEqual(
            page,
            [(1, ("gap", "s", 1, "sealed")),
             (2, ("message", "s", 2, b"XYZ"))],
        )

        # Commit succeeds but this caller never receives the receipt.
        original_hook = delivery_spool._after_commit

        def lose_receipt(path):
            raise _ReceiptLost("receipt lost after commit")

        delivery_spool._after_commit = lose_receipt
        try:
            with self.assertRaises(_ReceiptLost):
                spool.submit(2, group_two)
        finally:
            delivery_spool._after_commit = original_hook

        spool = self.reopen()
        self.assertEqual(spool.highest_delivery, 3)
        self.assertEqual(spool.usage, 8)
        replay = spool.submit(2, group_two)  # identical content again
        self.assertEqual(
            (replay.first_delivery, replay.last_delivery), (3, 3)
        )
        page = spool.read("r1", 100)
        self.assertEqual(
            page,
            [(1, ("gap", "s", 1, "sealed")),
             (2, ("message", "s", 2, b"XYZ")),
             (3, ("message", "t", 1, b"WXYZ"))],
        )  # exactly one copy, not two

        # A one-byte change under the latest number is clearly rejected.
        with open(self.path, "rb") as handle:
            bytes_before_conflict = handle.read()
        altered = (("message", "t", 1, b"WXYz"),)
        with self.assertRaises(GroupConflict):
            spool.submit(2, altered)
        with open(self.path, "rb") as handle:
            self.assertEqual(handle.read(), bytes_before_conflict)
        self.assertEqual(spool.read("r1", 100), page)
        self.assertEqual(spool.usage, 8)

        # Reclaim everything, reopen, and replay the latest number again.
        spool.confirm("r1", 3)
        result = spool.reclaim()
        self.assertEqual(
            (result.reclaimed_through, result.freed_bytes, result.usage),
            (3, 8, 0),
        )
        self.assertEqual(spool.highest_delivery, 3)
        spool = self.reopen()
        self.assertEqual(spool.read("r1", 100), [])
        self.assertEqual(spool.usage, 0)
        again = spool.submit(2, group_two)
        self.assertEqual(
            (again.first_delivery, again.last_delivery, again.event_count),
            (3, 3, 1),
        )
        self.assertEqual(spool.read("r1", 100), [])  # no resurrection
        self.assertEqual(spool.highest_delivery, 3)
        with self.assertRaises(StaleGroup):
            spool.submit(1, group_one)
        print(
            "\n[delivery scenario 1 observed] interrupted_write="
            f"SpoolWriteError (replace attempts={failing.attempts}); "
            f"committed_range_after_retry={receipt.first_delivery}-"
            f"{receipt.last_delivery}; lost_receipt_replay="
            f"{replay.first_delivery}-{replay.last_delivery}; "
            f"conflict_on_altered_byte=GroupConflict; "
            f"reclaim=through {result.reclaimed_through} freed "
            f"{result.freed_bytes} bytes; post_reopen_replay="
            f"{again.first_delivery}-{again.last_delivery}; "
            f"usage={spool.usage}; deliveries_read={len(spool.read('r1', 100))}",
            flush=True,
        )


class ScenarioTwoSlowReaderTest(_TempRoot):
    """Two readers, capacity 8, one trailing acknowledgement behind."""

    EVENTS = (
        ("message", "east", 1, b"ABCD"),
        ("gap", "east", 2, "sealed"),
        ("message", "east", 3, b"XYZ"),
    )
    NEXT = (("message", "east", 4, b"WXYZ"),)

    def _prime(self):
        spool = DeliverySpool.create(self.path, ["a", "b"], 8)
        receipt = spool.submit(1, self.EVENTS)
        self.assertEqual(
            (receipt.first_delivery, receipt.last_delivery), (1, 3)
        )
        self.assertEqual(spool.usage, 8)
        self.assertEqual(len(spool.read("a", 10)), 3)
        self.assertEqual(len(spool.read("b", 10)), 3)
        self.assertEqual(spool.confirm("a", 3), 3)
        self.assertEqual(spool.cursor("b"), 0)  # read, not confirmed
        return spool

    def test_capacity_held_until_slowest_reader_confirms(self):
        spool = self._prime()
        usage_before = spool.usage
        with self.assertRaises(NothingToReclaim) as empty:
            spool.reclaim()
        self.assertEqual(spool.usage, usage_before)  # no prefix freed

        with self.assertRaises(CapacityExceeded) as caught:
            spool.submit(2, self.NEXT)
        self.assertEqual(
            (caught.exception.requested, caught.exception.available),
            (4, 0),
        )
        self.assertEqual(spool.usage, 8)
        self.assertEqual(spool.next_group_number, 2)
        self.assertEqual(spool.highest_delivery, 3)

        # Retrying the latest already-committed group never needs capacity.
        replay = spool.submit(1, self.EVENTS)
        self.assertEqual(
            (replay.first_delivery, replay.last_delivery), (1, 3)
        )

        # Advancing only the slow reader to 1 frees exactly the 4-byte head.
        self.assertEqual(spool.confirm("b", 1), 1)
        result = spool.reclaim()
        self.assertEqual(
            (result.reclaimed_through, result.freed_bytes, result.usage),
            (1, 4, 4),
        )
        receipt = spool.submit(2, self.NEXT)
        self.assertEqual(
            (receipt.first_delivery, receipt.last_delivery), (4, 4)
        )
        self.assertEqual(spool.usage, 8)

        # Reopen: each reader resumes from its own cursor.
        spool = self.reopen()
        self.assertEqual(
            (spool.cursor("a"), spool.cursor("b")), (3, 1)
        )
        b_page = spool.read("b", 10)
        a_page = spool.read("a", 10)
        self.assertEqual(
            b_page,
            [
                (2, ("gap", "east", 2, "sealed")),
                (3, ("message", "east", 3, b"XYZ")),
                (4, ("message", "east", 4, b"WXYZ")),
            ],
        )
        self.assertEqual(a_page, [(4, ("message", "east", 4, b"WXYZ"))])
        print(
            "\n[delivery scenario 2 observed] empty_reclaim="
            f"{type(empty.exception).__name__}; "
            f"rejected_submit=CapacityExceeded(requested="
            f"{caught.exception.requested}, available="
            f"{caught.exception.available}); usage_before={usage_before}; "
            f"usage_after_reclaim={result.usage}; "
            f"freed_events={result.freed_events}; freed_bytes="
            f"{result.freed_bytes}; next_receipt={receipt.first_delivery}-"
            f"{receipt.last_delivery}; "
            f"reopened_cursors=a:{spool.cursor('a')},b:{spool.cursor('b')}; "
            f"b_reads={[item[0] for item in b_page]}; "
            f"a_reads={[item[0] for item in a_page]}",
            flush=True,
        )


class ScenarioThreeFailedReclaimTest(_TempRoot):
    """Confirmation commits independently; the deletion write then fails."""

    EVENTS = ScenarioTwoSlowReaderTest.EVENTS
    NEXT = ScenarioTwoSlowReaderTest.NEXT

    def test_failed_reclaim_keeps_confirmation_and_capacity(self):
        spool = DeliverySpool.create(self.path, ["a", "b"], 8)
        spool.submit(1, self.EVENTS)
        spool.read("a", 10)
        spool.read("b", 10)
        spool.confirm("a", 3)
        self.assertEqual(spool.cursor("b"), 0)
        with self.assertRaises(NothingToReclaim) as nothing:
            spool.reclaim()  # distinguish "no prefix" from a failed write
        self.assertNotIsInstance(nothing.exception, SpoolWriteError)

        # Confirmation of the slow reader commits on its own ...
        self.assertEqual(spool.confirm("b", 1), 1)

        # ... and only the subsequent explicit reclamation fails to persist.
        original_replace = delivery_spool._atomic_replace
        failing = _FailingReplace()
        delivery_spool._atomic_replace = failing
        try:
            with self.assertRaises(SpoolWriteError) as caught:
                spool.reclaim()
        finally:
            delivery_spool._atomic_replace = original_replace
        self.assertEqual(failing.attempts, 1)
        self.assertNotIsInstance(caught.exception, NothingToReclaim)

        # Reopen: confirmation survived, nothing was deleted, no space freed.
        spool = self.reopen()
        cursor_b_after = spool.cursor("b")
        reclaimed_after = spool.reclaimed_through
        usage_after_failure = spool.usage
        self.assertEqual(cursor_b_after, 1)
        self.assertEqual(reclaimed_after, 0)
        self.assertEqual(usage_after_failure, 8)
        b_reads_after = [item[0] for item in spool.read("b", 10)]
        self.assertEqual(b_reads_after, [2, 3])
        with self.assertRaises(CapacityExceeded):
            spool.submit(2, self.NEXT)  # capacity was not loosened
        self.assertEqual(spool.usage, 8)

        # Retry only the reclamation, then the previously rejected group fits.
        result = spool.reclaim()
        self.assertEqual(
            (result.reclaimed_through, result.freed_bytes, result.usage),
            (1, 4, 4),
        )
        receipt = spool.submit(2, self.NEXT)
        self.assertEqual(
            (receipt.first_delivery, receipt.last_delivery), (4, 4)
        )
        cursor_after_retry = spool.cursor("b")
        b_reads_after_retry = [item[0] for item in spool.read("b", 10)]
        self.assertEqual(cursor_after_retry, 1)  # no re-confirmation needed
        self.assertEqual(b_reads_after_retry, [2, 3, 4])
        print(
            "\n[delivery scenario 3 observed] empty_reclaim="
            f"{type(nothing.exception).__name__}; failed_reclaim="
            f"{type(caught.exception).__name__}; "
            f"replace_attempts={failing.attempts}; "
            f"cursor_b_after_reopen={cursor_b_after}; "
            f"reclaimed_through_after_failure={reclaimed_after}; "
            f"usage_after_failure={usage_after_failure}; "
            f"b_reads_after_reopen={b_reads_after}; retry_reclaim_freed="
            f"{result.freed_bytes} bytes; retry_receipt="
            f"{receipt.first_delivery}-{receipt.last_delivery}; "
            f"b_reads_after_retry={b_reads_after_retry}",
            flush=True,
        )


class DeliveryNumberTest(_TempRoot):
    def test_gap_counts_one_and_messages_count_payload_bytes(self):
        spool = DeliverySpool.create(self.path, ["r"], 100)
        spool.submit(
            1,
            (
                ("gap", "a", 1, "sealed"),
                ("message", "b", 1, b"abcde"),
                ("gap", "c", 4, "sealed"),
            ),
        )
        self.assertEqual(spool.usage, 1 + 5 + 1)
        page = spool.read("r", 2)
        self.assertEqual([item[0] for item in page], [1, 2])
        self.assertEqual(
            page,
            [(1, ("gap", "a", 1, "sealed")),
             (2, ("message", "b", 1, b"abcde"))],
        )
        self.assertEqual([item[0] for item in spool.read("r", 2)], [1, 2])
        self.assertEqual(spool.cursor("r"), 0)  # reads never confirm
        spool.confirm("r", 2)
        self.assertEqual(spool.cursor("r"), 2)
        self.assertEqual(spool.read("r", 10), [(3, ("gap", "c", 4, "sealed"))])

        # Re-confirming an older number is a no-op success.
        self.assertEqual(spool.confirm("r", 1), 2)
        self.assertEqual(spool.usage, 7)  # confirmation alone frees nothing

    def test_reclaim_only_common_prefix_and_no_partial_delete(self):
        spool = DeliverySpool.create(self.path, ["a", "b"], 100)
        spool.submit(
            1,
            (
                ("message", "s", 1, b"11"),
                ("message", "s", 2, b"222"),
                ("message", "s", 3, b"3333"),
            ),
        )
        spool.confirm("a", 2)
        spool.confirm("b", 1)
        result = spool.reclaim()
        self.assertEqual(
            (result.reclaimed_through, result.freed_events,
             result.freed_bytes, result.usage),
            (1, 1, 2, 7),  # 3 + 4 retained bytes remain
        )
        spool = self.reopen()
        self.assertEqual(spool.reclaimed_through, 1)
        self.assertEqual(spool.usage, 7)
        # a confirmed through 2, so it resumes at 3; b confirmed only 1.
        self.assertEqual(
            spool.read("a", 10),
            [(3, ("message", "s", 3, b"3333"))],
        )
        self.assertEqual(
            spool.read("b", 10),
            [(2, ("message", "s", 2, b"222")),
             (3, ("message", "s", 3, b"3333"))],
        )


class DemoRecordingTest(_TempRoot):
    """The receive demo can optionally record and later reopen its groups."""

    def test_demo_records_only_produced_groups_and_reopens(self):
        import demo_receive

        observed = demo_receive.run(spool_path=self.path)
        produced = [events for _, _, _, events, _ in observed if events]
        self.assertEqual(
            produced,
            [
                (("message", "west", 1, b"west"),),
                (("message", "east", 1, b"abcde"),
                 ("message", "east", 2, b"XYZ")),
            ],
        )  # the first two steps produced nothing and were not recorded

        spool = DeliverySpool.open(self.path)
        self.assertEqual(spool.readers, (demo_receive.COMPARATOR,))
        self.assertEqual(spool.committed_groups, len(produced))
        self.assertEqual(spool.highest_delivery, 3)
        self.assertEqual(
            spool.read(demo_receive.COMPARATOR, 10),
            [(1, ("message", "west", 1, b"west")),
             (2, ("message", "east", 1, b"abcde")),
             (3, ("message", "east", 2, b"XYZ"))],
        )

        reopened = DeliverySpool.open(self.path)
        self.assertEqual(reopened.cursor(demo_receive.COMPARATOR), 0)
        demo_receive.read_record(self.path)
        resumed = DeliverySpool.open(self.path)
        self.assertEqual(resumed.cursor(demo_receive.COMPARATOR), 2)
        self.assertEqual(resumed.reclaimed_through, 2)
        self.assertEqual(
            resumed.read(demo_receive.COMPARATOR, 10),
            [(3, ("message", "east", 2, b"XYZ"))],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
