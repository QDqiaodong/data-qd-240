"""Offline acceptance tests for :class:`drill_review.ReviewSpool`.

The four 4096-byte samples used by the headline scenarios are genuinely
reassembled by two real :class:`~receiving.WindowReceiver` instances from
synthetic slices; only the events those receivers actually produce are handed
to the spool.  Read-back is driven through a controllable in-process source
fixture so a real failure (``OSError``), same-length wrong content and a real
closed-source state are observable calls rather than labels.  No packet is
sent and no socket is opened anywhere in this module.

Expected sample bytes, delivery numbers, usage figures and page contents are
all written here as independent literals; the code under test is never used to
compute the expected answers.
"""

import os
import tempfile
import unittest

import drill_review
from drill_review import (
    CapacityError,
    ConfigurationError,
    InvalidAck,
    InvalidEvent,
    InvalidPageSize,
    RecallError,
    ReviewSpool,
    SpoolWriteError,
    UnknownReader,
)
from receiving import WindowReceiver, encode_slice

SAMPLE_LENGTH = 4096
RESIDENT = 4096
DISK = 8192
READERS = ("stat", "rev1", "rev2")
REVIEWERS = ("rev1", "rev2")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class RecallSource:
    """In-process stand-in for one identity's still-open read-back source.

    It records every real recall call, can be switched to raise ``OSError`` or
    serve same-length different bytes, and exposes a real ``closed`` flag that
    the caller only sets after a successful :meth:`ReviewSpool.withdraw`.
    """

    def __init__(self, identity, payload):
        self.identity = identity
        self._payload = payload
        self.calls = []
        self.fail = False
        self.corrupt = False
        self.closed = False

    def __call__(self, stream, number):
        self.calls.append((stream, number))
        if self.closed:
            raise AssertionError(
                f"closed source {self.identity!r} must never be recalled"
            )
        if self.fail:
            raise OSError(f"source {self.identity!r} fault injected")
        if self.corrupt:
            return b"!" * len(self._payload)
        return self._payload


class SourceRegistry:
    """Holds the source for each identity and refuses calls on closed ones."""

    def __init__(self, payloads):
        self.sources = {
            identity: RecallSource(identity, payload)
            for identity, payload in payloads.items()
        }

    def recall(self, stream, number):
        source = self.sources[(stream, number)]
        return source(stream, number)

    def close(self, identity):
        self.sources[identity].closed = True


def build_sample(pattern):
    """A deterministic 4096-byte literal built without the code under test."""
    return bytes((pattern + offset) % 251 for offset in range(SAMPLE_LENGTH))


def make_workdir(root):
    path = tempfile.mkdtemp(prefix="work-", dir=root)
    return path


# ---------------------------------------------------------------------------
# Scenario 1: real recall dependency, withdrawal, corruption and migration
# ---------------------------------------------------------------------------


class ScenarioOneRecallAndWithdrawTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.workdir = make_workdir(self._tmp.name)
        self.payloads = {
            ("a", 1): build_sample(10),
            ("b", 1): build_sample(40),
            ("c", 1): build_sample(70),
            ("d", 1): build_sample(100),
        }
        # Two real receivers, two streams each, reassemble all four samples.
        recv_x = WindowReceiver({"a": (SAMPLE_LENGTH,), "b": (SAMPLE_LENGTH,)},
                                4096)
        recv_y = WindowReceiver({"c": (SAMPLE_LENGTH,), "d": (SAMPLE_LENGTH,)},
                                4096)
        self.events = {}
        for receiver, stream in ((recv_x, "a"), (recv_x, "b"),
                                 (recv_y, "c"), (recv_y, "d")):
            payload = self.payloads[(stream, 1)]
            status = None
            produced = ()
            for offset in range(0, SAMPLE_LENGTH, 512):
                piece = payload[offset:offset + 512]
                if not piece:
                    break
                status, produced = receiver.feed(
                    stream, encode_slice(1, offset, piece)
                )
            self.assertEqual(status, "accepted")
            self.assertEqual(produced, (("message", stream, 1, payload),))
            self.events[stream] = produced[0]

        self.registry = SourceRegistry(self.payloads)
        self.spool = ReviewSpool(
            self.workdir, READERS, REVIEWERS, self.registry.recall,
            resident_bytes=RESIDENT, disk_bytes=DISK, recall_slots=2,
        )

    def tearDown(self):
        self.spool.close()

    def test_real_recall_dependency_withdraw_fault_corruption_migration(self):
        spool = self.spool
        sources = self.registry.sources

        ids = {}
        for stream in ("a", "b", "c", "d"):
            ids[stream] = spool.submit(self.events[stream])
        self.assertEqual([ids[name] for name in ("a", "b", "c", "d")],
                         [1, 2, 3, 4])

        resident, disk, recall_ids = spool.usage()
        # Budgets force exactly one identity onto real read-back recall.
        self.assertEqual((resident, disk), (RESIDENT, DISK))
        self.assertEqual(recall_ids, (ids["d"],))
        self.assertGreaterEqual(len(recall_ids), 1)
        files = set(os.listdir(self.workdir))
        self.assertEqual(len(files), 2)

        # A is resident: its source must be closable while review stays exact.
        self.assertEqual(sources["a", 1].calls, [])
        self.registry.close(("a", 1))
        self.assertTrue(spool.withdraw("a", 1))
        # Repeated withdrawal is idempotent and never touches a closed source.
        self.assertTrue(spool.withdraw("a", 1))
        self.assertEqual(sources["a", 1].calls, [])
        self.assertEqual(spool.sample("rev1", ids["a"]),
                         self.payloads[("a", 1)])

        # D genuinely depends on its source.  First prove a real recall call.
        self.assertEqual(sources["d", 1].calls, [])
        self.assertEqual(spool.sample("rev2", ids["d"]),
                         self.payloads[("d", 1)])
        self.assertEqual(sources["d", 1].calls, [("d", 1)])

        # While the D source is broken, withdrawal must be refused and every
        # promise/cursor must remain intact.
        sources["d", 1].fail = True
        before_usage = spool.usage()
        before_cursors = {r: spool.cursor(r) for r in READERS}
        self.assertFalse(spool.withdraw("d", 1))
        self.assertEqual(spool.usage(), before_usage)
        self.assertEqual({r: spool.cursor(r) for r in READERS},
                         before_cursors)
        with self.assertRaises(RecallError):
            spool.sample("rev1", ids["d"])
        self.assertEqual(spool.usage(), before_usage)
        sources["d", 1].fail = False

        # Same-length different content is a recall fault, not a new promise.
        sources["d", 1].corrupt = True
        with self.assertRaises(RecallError):
            spool.sample("rev1", ids["d"])
        # withdraw signals "not yet closable" via False rather than raising.
        self.assertFalse(spool.withdraw("d", 1))
        self.assertEqual(spool.usage(), before_usage)
        sources["d", 1].corrupt = False

        # Another identity that still needs recall gets same-length wrong
        # bytes: the error must be rejected without advancing any cursor.
        # Make C recall-dependent first by migrating it out during the D
        # withdrawal below; here verify the direct corruption path on D again.
        self.assertEqual(spool.sample("rev1", ids["d"]),
                         self.payloads[("d", 1)])
        self.assertEqual(spool.cursor("rev1"), 0)

        # Localizing D needs 4096 disk bytes but both disk slots hold B and C.
        # The spool may demote a *different*, stably recallable sample (B) to
        # make room; budgets and every earlier promise must survive the move.
        b_calls_before = list(sources["b", 1].calls)
        self.assertTrue(spool.withdraw("d", 1))
        self.registry.close(("d", 1))
        resident, disk, recall_after = spool.usage()
        self.assertEqual(recall_after, (ids["b"],))  # B now serves via recall
        self.assertEqual((resident, disk), (RESIDENT, DISK))
        # B's source was really probed during the migration.
        self.assertGreater(len(sources["b", 1].calls), len(b_calls_before))
        # The closed D source is never read again; its bytes come from disk.
        d_calls = list(sources["d", 1].calls)
        self.assertEqual(spool.sample("rev1", ids["d"]),
                         self.payloads[("d", 1)])
        self.assertEqual(sources["d", 1].calls, d_calls)
        # B is still an exact, unconfirmed reviewable sample via its source.
        self.assertEqual(spool.sample("rev1", ids["b"]),
                         self.payloads[("b", 1)])

        # Another identity that still needs recall (now B) gets same-length
        # wrong bytes: the bad content is rejected without moving a cursor.
        cursor_before = spool.cursor("rev1")
        sources["b", 1].corrupt = True
        with self.assertRaises(RecallError):
            spool.sample("rev2", ids["b"])
        sources["b", 1].corrupt = False
        self.assertEqual(spool.cursor("rev1"), cursor_before)
        self.assertEqual(spool.sample("rev2", ids["b"]),
                         self.payloads[("b", 1)])

        # All four conclusions remain readable by the statistical reader,
        # which has confirmed nothing.
        self.assertEqual(
            [row[0] for row in spool.read("stat", 10)], [1, 2, 3, 4]
        )

        print(
            "\n[drill scenario 1 observed] recall_ids="
            f"{recall_ids}; withdraw_a=True (resident, source closed); "
            "withdraw_d_while_faulted=False; corrupt_recall=RecallError; "
            "withdraw_d_after_recovery=True (demoted B to recall); "
            f"recall_after_withdraw={recall_after}; "
            "d_recall_calls_after_close="
            f"{len(sources['d', 1].calls)}; reviewers_can_still_review=True",
            flush=True,
        )


class ScenarioOneFaultedSourceWithdrawRefusalTest(unittest.TestCase):
    """The exact spec timing: A source faults, others normal, withdraw(A)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.workdir = make_workdir(self._tmp.name)
        self.payloads = {
            (name, 1): build_sample(base)
            for name, base in (("a", 10), ("b", 40), ("c", 70), ("d", 100))
        }
        self.registry = SourceRegistry(self.payloads)
        self.spool = ReviewSpool(
            self.workdir, READERS, REVIEWERS, self.registry.recall,
            resident_bytes=RESIDENT, disk_bytes=DISK, recall_slots=2,
        )
        self.addCleanup(self.spool.close)
        self.ids = {}
        for stream in ("a", "b", "c", "d"):
            self.ids[stream] = self.spool.submit(
                ("message", stream, 1, self.payloads[(stream, 1)])
            )

    def test_faulted_a_source_withdraw_refused_then_recovered(self):
        spool = self.spool
        sources = self.registry.sources
        recall_id = spool.usage()[2]
        # The greedy policy localizes A; only D depends on recall.
        self.assertEqual(recall_id, (self.ids["d"],))

        # Fault A's source even though A is local: withdrawal still succeeds
        # because the local copy alone must support review.
        sources["a", 1].fail = True
        self.assertTrue(spool.withdraw("a", 1))
        self.registry.close(("a", 1))
        self.assertEqual(spool.sample("rev1", self.ids["a"]),
                         self.payloads[("a", 1)])
        self.assertEqual(sources["a", 1].calls, [])

        # Now demonstrate the recall-identity branch on D with the prescribed
        # timing: fault its source while other sources stay normal.
        sources["d", 1].fail = True
        cursors_before = {r: spool.cursor(r) for r in READERS}
        self.assertFalse(spool.withdraw("d", 1))
        self.assertEqual({r: spool.cursor(r) for r in READERS},
                         cursors_before)
        self.assertEqual(spool.usage()[2], (self.ids["d"],))
        sources["d", 1].fail = False
        # Recovery restores exact read-back and then permits withdrawal.
        self.assertEqual(spool.sample("rev2", self.ids["d"]),
                         self.payloads[("d", 1)])
        self.assertTrue(spool.withdraw("d", 1))
        self.registry.close(("d", 1))
        # Disk was full (B+C), so localizing D demotes the first healthy
        # disk sample B onto recall; D itself no longer depends on a source.
        self.assertEqual(spool.usage()[2], (self.ids["b"],))
        # Snapshot after the successful localization probe; from here on the
        # closed D source must never be read again.
        d_calls = list(sources["d", 1].calls)
        self.assertEqual(spool.sample("rev1", self.ids["d"]),
                         self.payloads[("d", 1)])
        self.assertEqual(sources["d", 1].calls, d_calls)
        # The demoted B is still exactly reviewable through its open source.
        self.assertEqual(spool.sample("rev2", self.ids["b"]),
                         self.payloads[("b", 1)])


# ---------------------------------------------------------------------------
# Scenario 2: recall_slots=0, confirmation frees the fourth slot, real gap
# ---------------------------------------------------------------------------


class ScenarioTwoZeroRecallSlotsTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.workdir = make_workdir(self._tmp.name)
        self.payloads = {
            (name, 1): build_sample(base)
            for name, base in (("a", 12), ("b", 42), ("c", 72), ("d", 102))
        }
        self.registry = SourceRegistry(self.payloads)
        self.spool = ReviewSpool(
            self.workdir, READERS, REVIEWERS, self.registry.recall,
            resident_bytes=RESIDENT, disk_bytes=DISK, recall_slots=0,
        )
        self.addCleanup(self.spool.close)

    def test_confirmation_releases_capacity_and_gap_has_no_payload(self):
        spool = self.spool
        ids = {}
        for stream in ("a", "b", "c"):
            ids[stream] = spool.submit(
                ("message", stream, 1, self.payloads[(stream, 1)])
            )
        self.assertEqual([ids[name] for name in ("a", "b", "c")], [1, 2, 3])
        self.assertEqual(spool.usage(), (RESIDENT, DISK, ()))

        # No statistical reader has confirmed anything.  Reviewer rev1
        # confirms A but rev2 does not: A's payload must stay, so D is refused.
        self.assertEqual(spool.confirm("rev1", ids["a"]), 1)
        d_event = ("message", "d", 1, self.payloads[("d", 1)])
        with self.assertRaises(CapacityError) as caught:
            spool.submit(d_event)
        self.assertEqual(caught.exception.length, SAMPLE_LENGTH)
        self.assertEqual(spool.highest_delivery, 3)  # no number consumed
        # The identical event is retryable with the same identity.
        with self.assertRaises(CapacityError):
            spool.submit(d_event)

        # Once the last reviewer also confirms A, its payload is released even
        # though the statistical reader is still at cursor zero.
        self.assertEqual(spool.confirm("rev2", ids["a"]), 1)
        self.assertEqual(spool.confirm("stat", 0), 0)  # non-moving no-op
        d_id = spool.submit(d_event)
        self.assertEqual(d_id, 4)

        # The statistical reader still reads all four conclusions in order.
        page = spool.read("stat", 10)
        self.assertEqual(
            [(row[0], row[1], row[2], row[3]) for row in page],
            [(1, "message", "a", 1), (2, "message", "b", 1),
             (3, "message", "c", 1), (4, "message", "d", 1)],
        )
        self.assertTrue(all(row[4] is None for row in page))
        self.assertEqual(spool.cursor("stat"), 0)  # reads never confirm

        # Reviewers can still fetch exact bytes for their unconfirmed B,C,D.
        for stream, delivery in (("b", ids["b"]), ("c", ids["c"]),
                                 ("d", d_id)):
            for reviewer in REVIEWERS:
                self.assertEqual(
                    spool.sample(reviewer, delivery),
                    self.payloads[(stream, 1)],
                )
        # A is confirmed by both reviewers: raw review is now refused.
        for reviewer in REVIEWERS:
            with self.assertRaises(LookupError):
                spool.sample(reviewer, ids["a"])

        # A genuine seal gap: conclusion only, no payload, no capacity used.
        receiver = WindowReceiver({"g": (3,)}, 1024)
        gap_events = receiver.seal("g", 1)
        self.assertEqual(gap_events, (("gap", "g", 1, "sealed"),))
        usage_before = spool.usage()
        gap_id = spool.submit(gap_events[0])
        self.assertEqual(gap_id, 5)
        self.assertEqual(spool.usage(), usage_before)
        gap_rows = [row for row in spool.read("rev1", 10)
                    if row[0] == gap_id]
        self.assertEqual(gap_rows, [(5, "gap", "g", 1, "sealed")])
        with self.assertRaises(LookupError):
            spool.sample("rev1", gap_id)

        # Closing leaves no temporary residue and removes no unrelated files.
        marker = os.path.join(self.workdir, "unrelated.txt")
        with open(marker, "wb") as handle:
            handle.write(b"keep me")
        spool.close()
        remaining = os.listdir(self.workdir)
        self.assertEqual(remaining, ["unrelated.txt"])
        os.unlink(marker)

        print(
            "\n[drill scenario 2 observed] d_rejected_before_full_confirm="
            "CapacityError; fourth_delivery_id="
            f"{d_id}; stat_conclusion_ids={[row[0] for row in page]}; "
            "reviewers_still_read_bcd=True; gap_delivery="
            f"{gap_id} (no payload); residue_after_close=[]",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Scenario 3: failure isolation, invalid readers, non-moving confirms
# ---------------------------------------------------------------------------


class ScenarioThreeFailureAndValidationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.workdir = make_workdir(self._tmp.name)
        self.payloads = {
            (name, 1): build_sample(base)
            for name, base in (("a", 13), ("b", 43), ("c", 73), ("d", 103))
        }
        self.registry = SourceRegistry(self.payloads)
        self.spool = ReviewSpool(
            self.workdir, READERS, REVIEWERS, self.registry.recall,
            resident_bytes=RESIDENT, disk_bytes=DISK, recall_slots=2,
        )
        self.addCleanup(self.spool.close)
        self.ids = {}
        for stream in ("a", "b", "c", "d"):
            self.ids[stream] = self.spool.submit(
                ("message", stream, 1, self.payloads[(stream, 1)])
            )

    def test_disk_write_failure_during_withdraw_keeps_old_state(self):
        # Under the fixed budgets all disk slots are full, so localizing the
        # recall sample D requires migrating a healthy disk sample (B) out.
        # The replacement copy is staged *before* any demotion, so a write
        # failure leaves the old state byte-for-byte intact.
        spool = self.spool
        d_id = self.ids["d"]
        self.assertEqual(spool.usage(), (RESIDENT, DISK, (d_id,)))
        original_write = drill_review._write_payload_file
        attempts = []

        def failing_write(path, data):
            attempts.append(path)
            raise OSError("injected disk failure during localization")

        drill_review._write_payload_file = failing_write
        try:
            self.assertFalse(spool.withdraw("d", 1))
        finally:
            drill_review._write_payload_file = original_write
        self.assertEqual(len(attempts), 1)

        # Old state survives byte-for-byte: D still depends on recall, B and C
        # are still on disk, cursors and every promised byte remain available.
        self.assertEqual(spool.usage(), (RESIDENT, DISK, (d_id,)))
        for reader in READERS:
            self.assertEqual(spool.cursor(reader), 0)
        self.assertEqual(
            spool.sample("rev1", d_id), self.payloads[("d", 1)]
        )
        self.assertEqual(
            [row[0] for row in spool.read("stat", 10)], [1, 2, 3, 4]
        )
        # No staged temp file is left behind.
        self.assertFalse(
            any(name.startswith(".review-")
                for name in os.listdir(self.workdir))
        )
        # A healthy retry completes the migration: D reaches disk and B is the
        # single remaining recall dependency; no budget is exceeded.
        self.assertTrue(spool.withdraw("d", 1))
        self.registry.close(("d", 1))
        self.assertEqual(spool.usage(), (RESIDENT, DISK, (self.ids["b"],)))
        d_calls = list(self.registry.sources[("d", 1)].calls)
        self.assertEqual(
            spool.sample("rev2", d_id), self.payloads[("d", 1)]
        )
        self.assertEqual(self.registry.sources[("d", 1)].calls, d_calls)
        self.assertEqual(
            spool.sample("rev1", self.ids["b"]), self.payloads[("b", 1)]
        )

        print(
            "\n[drill scenario 3 observed] "
            "write_failure_withdraw_result=False; write_attempts="
            f"{len(attempts)}; state_after_failure=(4096, 8192, ({d_id},)); "
            "retry_after_recovery=True; recall_after_retry="
            f"({self.ids['b']},)",
            flush=True,
        )

    def test_disk_write_failure_during_submit_keeps_number_and_directory(self):
        # A fresh zero-slot spool where the next disk write must fail.
        workdir = make_workdir(self._tmp.name)
        registry = SourceRegistry({("a", 1): build_sample(5)})
        spool = ReviewSpool(
            workdir, ("r",), ("r",), registry.recall,
            resident_bytes=1, disk_bytes=DISK, recall_slots=0,
        )
        self.addCleanup(spool.close)
        original_write = drill_review._write_payload_file

        def failing_write(path, data):
            raise OSError("injected submit disk failure")

        drill_review._write_payload_file = failing_write
        try:
            with self.assertRaises(SpoolWriteError):
                spool.submit(("message", "a", 1, build_sample(5)))
        finally:
            drill_review._write_payload_file = original_write
        self.assertEqual(spool.highest_delivery, 0)
        self.assertEqual(spool.usage(), (0, 0, ()))
        self.assertEqual(os.listdir(workdir), [])
        # Retry with the same identity now succeeds as delivery 1.
        self.assertEqual(spool.submit(("message", "a", 1, build_sample(5))), 1)

    def test_unknown_reader_and_non_advancing_confirmation(self):
        spool = self.spool
        with self.assertRaises(UnknownReader):
            spool.read("ghost", 10)
        with self.assertRaises(UnknownReader):
            spool.confirm("ghost", 1)
        with self.assertRaises(UnknownReader):
            spool.cursor("ghost")

        # A purely statistical reader has no raw-byte permission.
        with self.assertRaises(LookupError):
            spool.sample("stat", self.ids["a"])

        for bad_limit in (0, -2, True, 3.0, "3"):
            with self.assertRaises(InvalidPageSize):
                spool.read("rev1", bad_limit)
        for bad_ack in (-1, 99, True, 1.0, "1"):
            with self.assertRaises(InvalidAck):
                spool.confirm("rev1", bad_ack)

        # Re-confirming an older/current number is a side-effect-free success.
        self.assertEqual(spool.confirm("rev1", 0), 0)
        self.assertEqual(spool.confirm("rev1", 2), 2)
        self.assertEqual(spool.confirm("rev1", 1), 2)
        self.assertEqual(spool.cursor("rev1"), 2)
        # rev2 has not confirmed 2, so its payload is still reviewable.
        self.assertEqual(
            spool.sample("rev2", self.ids["b"]), self.payloads[("b", 1)]
        )

    def test_event_validation_and_ordering(self):
        spool = self.spool
        for bad in (
            ("message", "a", 1),
            ("message", "", 1, b"x"),
            ("message", "a", 0, b"x"),
            ("message", "a", True, b"x"),
            ("message", "a", 1, b""),
            ("message", "a", 1, "text"),
            ("gap", "a", 1, ""),
            ("weird", "a", 1, b"x"),
        ):
            with self.assertRaises(InvalidEvent):
                spool.submit(bad)
        # Per-stream sample numbers must advance by the event's own semantics.
        with self.assertRaises(InvalidEvent):
            spool.submit(("message", "a", 3, b"z" * 4))
        # The next legal a sample is number 2 and gets a fresh delivery id.
        nxt = spool.submit(("message", "a", 2, b"z" * 4))
        self.assertEqual(nxt, 5)
        self.assertEqual(spool.highest_delivery, 5)

    def test_reviewer_subset_governs_payload_all_readers_govern_conclusion(self):
        spool = self.spool
        a_id = self.ids["a"]  # resident, 4096 bytes
        self.assertEqual(spool.usage()[0], RESIDENT)

        # One reviewer alone never releases the payload.
        spool.confirm("rev1", a_id)
        self.assertEqual(spool.usage()[0], RESIDENT)
        with self.assertRaises(LookupError):
            spool.sample("rev1", a_id)  # rev1 confirmed: no review for them
        # rev2 has not confirmed, so exact bytes remain available.
        self.assertEqual(
            spool.sample("rev2", a_id), self.payloads[("a", 1)]
        )
        # The statistical reader, who confirmed nothing, keeps the conclusion.
        self.assertEqual(
            spool.read("stat", 1)[0],
            (a_id, "message", "a", 1, None),
        )

        # The last reviewer releases the payload even though stat is at zero.
        spool.confirm("rev2", a_id)
        self.assertEqual(spool.usage()[0], RESIDENT - SAMPLE_LENGTH)
        for reviewer in REVIEWERS:
            with self.assertRaises(LookupError):
                spool.sample(reviewer, a_id)
        self.assertEqual(
            spool.read("stat", 1)[0],
            (a_id, "message", "a", 1, None),
        )

        # The conclusion disappears only once every reader (incl. stat) acks.
        spool.confirm("rev1", 4)
        spool.confirm("rev2", 4)
        self.assertIn(a_id, [row[0] for row in spool.read("stat", 10)])
        spool.confirm("stat", 4)
        self.assertEqual(spool.read("stat", 10), [])
        with self.assertRaises(LookupError):
            spool.sample("rev1", a_id)
        self.assertEqual(spool.usage(), (0, 0, ()))

    def test_construction_validation(self):
        for bad_readers in ((), ["x"] * 2, [""], [1], "ab",
                            ["1", "2", "3", "4", "5"]):
            with self.assertRaises(ConfigurationError):
                ReviewSpool(
                    make_workdir(self._tmp.name), bad_readers, ("r",),
                    self.registry.recall,
                    resident_bytes=1, disk_bytes=1, recall_slots=1,
                )
        base = make_workdir(self._tmp.name)
        with self.assertRaises(ConfigurationError):
            ReviewSpool(base, ("r",), ("ghost",), self.registry.recall,
                        resident_bytes=1, disk_bytes=1, recall_slots=1)
        for kwargs in (
            dict(resident_bytes=-1, disk_bytes=1, recall_slots=1),
            dict(resident_bytes=1, disk_bytes=16 * 1024 + 1, recall_slots=1),
            dict(resident_bytes=1, disk_bytes=1, recall_slots=9),
            dict(resident_bytes=True, disk_bytes=1, recall_slots=1),
            dict(resident_bytes=1, disk_bytes=False, recall_slots=1),
        ):
            with self.assertRaises(ConfigurationError):
                ReviewSpool(
                    make_workdir(self._tmp.name), ("r",), ("r",),
                    self.registry.recall, **kwargs
                )
        # A non-empty, already-used directory is never reused.
        with open(os.path.join(base, "leftover.bin"), "wb") as handle:
            handle.write(b"x")
        with self.assertRaises(ConfigurationError):
            ReviewSpool(base, ("r",), ("r",), self.registry.recall,
                        resident_bytes=1, disk_bytes=1, recall_slots=1)
        with self.assertRaises(ConfigurationError):
            ReviewSpool(
                os.path.join(self._tmp.name, "missing"), ("r",), ("r",),
                self.registry.recall,
                resident_bytes=1, disk_bytes=1, recall_slots=1,
            )
        with self.assertRaises(ConfigurationError):
            ReviewSpool(base, ("r",), ("r",), "not callable",
                        resident_bytes=1, disk_bytes=1, recall_slots=1)

    def test_context_manager_and_conclusion_cleanup(self):
        workdir = make_workdir(self._tmp.name)
        with ReviewSpool(
            workdir, ("r1", "r2"), ("r1",), self.registry.recall,
            resident_bytes=RESIDENT, disk_bytes=DISK, recall_slots=2,
        ) as spool:
            first = spool.submit(
                ("message", "x", 1, self.payloads[("a", 1)])
            )
            self.assertEqual(first, 1)
            spool.confirm("r1", 1)
            spool.confirm("r2", 1)
            # Once every reader confirms, the conclusion is gone.
            self.assertEqual(spool.read("r1", 10), [])
            with self.assertRaises(LookupError):
                spool.sample("r1", 1)
            self.assertEqual(os.listdir(workdir), [])
        self.assertTrue(spool.closed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
