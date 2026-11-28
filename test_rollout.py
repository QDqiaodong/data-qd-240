"""Acceptance tests for the offline slot-bounded image rollout.

All addresses, groups and rows are synthetic.  The simulated driver below opens
no sockets and touches no real device; it only mutates an in-memory slot bank
and records every physical write.  Expected revision-2 answers are written out
as independent literals, never derived from the planner's output.
"""

import unittest
from ipaddress import IPv4Address, IPv4Network

from rollout import NoReadyImage, NoSafeLayout, Rollout


RED = "red"
BLUE = "blue"

# Revision 1: one /24 per group.
OLD = {
    RED: (("198.51.100.0/24", "sent", "red-old"),),
    BLUE: (("203.0.113.0/24", "drop", None),),
}

# Revision 2: each old /24 replaced by two /25 halves with different results.
NEW = {
    RED: (
        ("198.51.100.0/25", "sent", "red-low"),
        ("198.51.100.128/25", "drop", None),
    ),
    BLUE: (
        ("203.0.113.0/25", "sent", "blue-low"),
        ("203.0.113.128/25", "sent", "blue-high"),
    ),
}

# Independent literal probes (address, expected (action, egress)).
OLD_PROBES = {
    RED: ("198.51.100.7", ("sent", "red-old")),
    BLUE: ("203.0.113.7", ("drop", None)),
}
NEW_PROBES = {
    RED: (
        ("198.51.100.0", ("sent", "red-low")),
        ("198.51.100.128", ("drop", None)),
        ("198.51.100.200", ("drop", None)),
        ("192.0.2.1", ("punt", None)),
    ),
    BLUE: (
        ("203.0.113.0", ("sent", "blue-low")),
        ("203.0.113.128", ("sent", "blue-high")),
        ("203.0.113.200", ("sent", "blue-high")),
        ("192.0.2.1", ("punt", None)),
    ),
}

OLD_PHYSICAL = {
    physical
    for group, rows in OLD.items()
    for physical in ((group,) + row for row in rows)
}
NEW_PHYSICAL = {
    physical
    for group, rows in NEW.items()
    for physical in ((group,) + row for row in rows)
}
ALL_PHYSICAL = OLD_PHYSICAL | NEW_PHYSICAL


def physical(row, group):
    return None if row is None else (group,) + tuple(row)


class FakeDevice:
    """In-memory slot bank with per-device introspection fault injection."""

    def __init__(self, slots, initial):
        self.slots = dict(slots)
        self.banks = {}
        for name, count in self.slots.items():
            rows = list(initial.get(name, ()))
            rows.extend([None] * (count - len(rows)))
            self.banks[name] = rows
        self.inspect_failures = set()
        self.writes = []       # (bank, index, row) actually applied
        self.programs = []     # (bank, index, row) requested
        self.after_write = None

    def fail_inspect(self, bank):
        self.inspect_failures.add(bank)

    def recover_inspect(self, bank):
        self.inspect_failures.discard(bank)

    def inspect(self, bank):
        if bank in self.inspect_failures:
            raise OSError(f"inspect unavailable on {bank}")
        return tuple(self.banks[bank])

    def program(self, bank, index, row):
        self.programs.append((bank, index, row))
        # The simulated slot really changes before any callback or return.
        self.banks[bank][index] = row
        self.writes.append((bank, index, row))
        if self.after_write is not None:
            self.after_write(bank, index, row)

    def lookup(self, bank, group, address):
        target = IPv4Address(address)
        best = None
        best_len = -1
        for row in self.banks[bank]:
            if row is None or row[0] != group:
                continue
            network = IPv4Network(row[1], strict=True)
            if target in network and network.prefixlen > best_len:
                best = (row[2], row[3])
                best_len = network.prefixlen
        return best if best is not None else ("punt", None)


SLOTS = {"A": 2, "B": 2, "C": 2}


def initial_layout():
    return {
        "A": [physical(OLD[RED][0], RED), physical(OLD[BLUE][0], BLUE)],
        "B": [physical(OLD[BLUE][0], BLUE), physical(OLD[RED][0], RED)],
        "C": [None, None],
    }


def group_image(banks, bank, group):
    return frozenset(
        row[1:] for row in banks[bank] if row is not None and row[0] == group
    )


class RolloutAcceptanceTest(unittest.TestCase):
    def make(self, eligible):
        device = FakeDevice(SLOTS, initial_layout())
        rollout = Rollout(device, SLOTS, eligible, OLD, NEW)
        return device, rollout

    def assert_old_readable(self, rollout, device, forbidden_banks=()):
        banks = []
        for group, (address, expected) in OLD_PROBES.items():
            revision, bank, action, egress = rollout.resolve(group, address)
            self.assertEqual(revision, 1)
            self.assertEqual((action, egress), expected)
            self.assertNotIn(bank, forbidden_banks)
            # The chosen device physically holds exactly the complete old image.
            self.assertEqual(
                group_image(device.banks, bank, group), frozenset(OLD[group])
            )
            banks.append(bank)
        return tuple(banks)

    def assert_new_readable(self, rollout, device):
        results = {}
        for group, probes in NEW_PROBES.items():
            for address, expected in probes:
                revision, bank, action, egress = rollout.resolve(group, address)
                self.assertEqual(revision, 2)
                self.assertEqual((action, egress), expected)
                self.assertEqual(
                    group_image(device.banks, bank, group),
                    frozenset(NEW[group]),
                )
                results[(group, address)] = (bank, action, egress)
        return results

    # -- Acceptance 1a ------------------------------------------------------
    def test_final_fit_is_not_enough_when_c_is_unauthorized(self):
        device, rollout = self.make(eligible=("A", "B"))

        with self.assertRaises(NoSafeLayout):
            rollout.publish()

        self.assertEqual(device.writes, [])
        self.assertEqual(device.programs, [])
        self.assertEqual(rollout.revision, 1)
        banks = self.assert_old_readable(rollout, device)
        print(
            "[rollout scenario 1a observed] rejected=NoSafeLayout; "
            f"physical_writes={len(device.writes)}; "
            f"old_read_banks={banks}; revision={rollout.revision}"
        )

    # -- Acceptance 1b ------------------------------------------------------
    def test_empty_c_eligible_publishes_safely(self):
        # A, B and the empty C are all available: C is the staging space that
        # lets both old images survive every intermediate layout.
        device, rollout = self.make(eligible=("A", "B", "C"))

        callback_banks = []

        def after_write(bank, index, row):
            # After every single slot write both groups must still answer at
            # revision 1 from a device that physically holds the whole old image.
            callback_banks.append(self.assert_old_readable(rollout, device))

        device.after_write = after_write
        revision = rollout.publish()

        self.assertEqual(revision, 2)
        self.assertGreater(len(device.writes), 0)
        # Only complete target rows were programmed; no old row was cleared and
        # no slot was set to None.
        for row in device.writes:
            self.assertIsNotNone(row[2])
            self.assertIn(row[2], NEW_PHYSICAL)
        # Residual old rows are left in place, never rewritten to the other rev.
        self.assertIn(physical(OLD[RED][0], RED), ALL_PHYSICAL)
        for bank in device.slots:
            for row in device.banks[bank]:
                self.assertTrue(row is None or row in ALL_PHYSICAL)

        results = self.assert_new_readable(rollout, device)
        print(
            "[rollout scenario 1b observed] physical_writes="
            f"{len(device.writes)}; writes={device.writes}; "
            f"callback_old_banks={callback_banks}; "
            f"new_reads_red={results[(RED, '198.51.100.0')]},"
            f"{results[(RED, '198.51.100.128')]},"
            f"{results[(RED, '192.0.2.1')]}; "
            f"new_reads_blue={results[(BLUE, '203.0.113.0')]},"
            f"{results[(BLUE, '203.0.113.128')]},"
            f"{results[(BLUE, '192.0.2.1')]}"
        )

    # -- Acceptance 2 -------------------------------------------------------
    def test_lost_ack_after_damaging_write_never_serves_broken_old(self):
        device, rollout = self.make(eligible=("A", "B", "C"))
        mirror = {bank: list(rows) for bank, rows in device.banks.items()}
        trigger = {}

        def complete_old(banks, bank, group):
            return frozenset(
                row[1:] for row in banks[bank]
                if row is not None and row[0] == group
            ) == frozenset(OLD[group])

        def after_write(bank, index, row):
            prior = mirror[bank][index]
            before = {
                group: complete_old(mirror, bank, group) for group in OLD
            }
            mirror[bank][index] = row
            if trigger:
                return
            # The first write that leaves a device (A or B) without a complete
            # old image it held just before this write.  The slot really
            # changed; only the acknowledgement is lost.
            if bank in ("A", "B") and any(
                before[group] and not complete_old(mirror, bank, group)
                for group in OLD
            ):
                trigger.update(bank=bank, index=index, row=row, prior=prior)
                device.fail_inspect(bank)
                # At the moment of the lost acknowledgement readers must
                # already be served revision 1 from an intact device.
                trigger["during_failure"] = {
                    group: rollout.resolve(group, OLD_PROBES[group][0])
                    for group in OLD
                }
                for group, probe in OLD_PROBES.items():
                    revision, read_bank, action, egress = trigger[
                        "during_failure"
                    ][group]
                    self.assertEqual(revision, 1)
                    self.assertEqual((action, egress), probe[1])
                    self.assertEqual(
                        group_image(device.banks, read_bank, group),
                        frozenset(OLD[group]),
                    )
                raise OSError("simulated lost acknowledgement")

        device.after_write = after_write
        with self.assertRaises(OSError) as caught:
            rollout.publish()

        self.assertEqual(str(caught.exception), "simulated lost acknowledgement")
        self.assertTrue(trigger, "test setup: no damaging write was performed")
        bank, index, row = trigger["bank"], trigger["index"], trigger["row"]

        # The write took effect physically.
        self.assertEqual(device.banks[bank][index], row)
        self.assertEqual(rollout.revision, 1)
        self.assertIn(bank, device.inspect_failures)

        programs_at_failure = len(device.programs)
        self.assertEqual(programs_at_failure, len(device.writes))

        # Readers still get revision 1 from an intact device; the damaged one
        # is skipped because its introspection is unavailable.
        banks = self.assert_old_readable(rollout, device, forbidden_banks=(bank,))
        # No further programming happened while inspecting the failure.
        self.assertEqual(len(device.programs), programs_at_failure)

        # Recover introspection and retry the same instance on real slots.
        device.recover_inspect(bank)
        device.after_write = None
        revision = rollout.publish()
        self.assertEqual(revision, 2)
        results = self.assert_new_readable(rollout, device)
        print(
            "[rollout scenario 2 observed] failed_write="
            f"({bank},{index}) prior={trigger['prior']}; "
            f"programs_at_failure={programs_at_failure}; "
            f"revision_after_failure=1; old_read_banks_after_failure={banks}; "
            f"revision_after_retry={revision}; "
            f"retry_red={results[(RED, '198.51.100.0')]},"
            f"{results[(RED, '198.51.100.128')]},"
            f"{results[(RED, '192.0.2.1')]}; "
            f"retry_blue={results[(BLUE, '203.0.113.0')]},"
            f"{results[(BLUE, '203.0.113.128')]},"
            f"{results[(BLUE, '192.0.2.1')]}"
        )


class RolloutLayoutTest(unittest.TestCase):
    """Shared physical rows are reused; row order is irrelevant."""

    def test_identical_row_satisfies_both_revisions(self):
        # The blue target keeps blue's old row verbatim, and red's target keeps
        # red's old /24 on a further device.  A single physical copy may serve
        # both revisions; it must not be rewritten or duplicated.
        active = {
            RED: (("198.51.100.0/24", "sent", "red-old"),),
            BLUE: (("203.0.113.0/24", "sent", "shared"),),
        }
        target = {
            RED: (
                ("198.51.100.0/25", "sent", "red-low"),
                ("198.51.100.128/25", "sent", "red-high"),
            ),
            BLUE: (("203.0.113.0/24", "sent", "shared"),),
        }
        initial = {
            "A": [
                ("red", "198.51.100.0/24", "sent", "red-old"),
                ("blue", "203.0.113.0/24", "sent", "shared"),
            ],
            "B": [None, None],
        }
        device = FakeDevice({"A": 2, "B": 2}, initial)
        rollout = Rollout(device, {"A": 2, "B": 2}, ("A", "B"), active, target)

        self.assertEqual(rollout.publish(), 2)
        # Exactly one physical copy of the shared blue row exists; it was not
        # replaced by an identical-looking but different row.
        shared = ("blue", "203.0.113.0/24", "sent", "shared")
        self.assertEqual(
            sum(row == shared for bank in device.banks.values() for row in bank),
            1,
        )
        self.assertNotIn(shared, [write[2] for write in device.writes])
        for address, expected in (
            ("203.0.113.9", ("sent", "shared")),
            ("198.51.100.0", ("sent", "red-low")),
            ("198.51.100.200", ("sent", "red-high")),
            ("192.0.2.9", ("punt", None)),
        ):
            group = BLUE if address.startswith("203.") else RED
            revision, bank, action, egress = rollout.resolve(group, address)
            self.assertEqual(revision, 2)
            self.assertEqual((action, egress), expected)

    def test_single_slot_devices_need_external_staging(self):
        # Two one-slot devices each hold one old row.  Red gets a new row while
        # blue's row is identical in both revisions.  Replacing A would destroy
        # the only copy of red's old image, so no safe order exists until a
        # third empty one-slot device can hold a spare copy while A changes.
        active = {
            RED: (("198.51.100.0/24", "sent", "red-old"),),
            BLUE: (("203.0.113.0/24", "sent", "shared"),),
        }
        target = {
            RED: (("198.51.100.0/24", "sent", "red-new"),),
            BLUE: (("203.0.113.0/24", "sent", "shared"),),
        }
        initial = {
            "A": [("red", "198.51.100.0/24", "sent", "red-old")],
            "B": [("blue", "203.0.113.0/24", "sent", "shared")],
        }
        slots = {"A": 1, "B": 1}
        device = FakeDevice(slots, initial)
        rollout = Rollout(device, slots, ("A", "B"), active, target)
        with self.assertRaises(NoSafeLayout):
            rollout.publish()
        self.assertEqual(device.writes, [])

        slots = {"A": 1, "B": 1, "C": 1}
        device = FakeDevice(slots, initial)
        rollout = Rollout(device, slots, ("A", "B", "C"), active, target)
        self.assertEqual(rollout.publish(), 2)
        for address, expected, group in (
            ("198.51.100.9", ("sent", "red-new"), RED),
            ("203.0.113.9", ("sent", "shared"), BLUE),
            ("192.0.2.9", ("punt", None), RED),
        ):
            revision, bank, action, egress = rollout.resolve(group, address)
            self.assertEqual(revision, 2)
            self.assertEqual((action, egress), expected)


    def test_planner_backtracks_when_first_arrangement_fails(self):
        # Red has two target rows; only C has room for both after blue finishes
        # moving.  A greedy "fill A first" order would strand red's second row,
        # so the planner must back off and use C.
        active = {
            RED: (("198.51.100.0/24", "sent", "red-old"),),
            BLUE: (("203.0.113.0/24", "sent", "blue-old"),),
        }
        target = {
            RED: (
                ("198.51.100.0/25", "sent", "red-low"),
                ("198.51.100.128/25", "drop", None),
            ),
            BLUE: (
                ("203.0.113.0/25", "sent", "blue-low"),
                ("203.0.113.128/25", "sent", "blue-high"),
            ),
        }
        initial = {
            "A": [("red", "198.51.100.0/24", "sent", "red-old")],
            "B": [("blue", "203.0.113.0/24", "sent", "blue-old")],
            "C": [None, None],
        }
        device = FakeDevice(SLOTS, initial)
        rollout = Rollout(device, SLOTS, ("A", "B", "C"), active, target)
        self.assertEqual(rollout.publish(), 2)

        # Four target rows need three devices' worth of slots plus staging for
        # the old images; verify every revision-2 answer is exact.
        for group, probes in (
            (RED, (("198.51.100.0", ("sent", "red-low")),
                   ("198.51.100.128", ("drop", None)),
                   ("192.0.2.1", ("punt", None)))),
            (BLUE, (("203.0.113.0", ("sent", "blue-low")),
                    ("203.0.113.128", ("sent", "blue-high")),
                    ("192.0.2.1", ("punt", None)))),
        ):
            for address, expected in probes:
                revision, bank, action, egress = rollout.resolve(group, address)
                self.assertEqual(revision, 2)
                self.assertEqual((action, egress), expected)


class RolloutValidationTest(unittest.TestCase):
    def make(self, **overrides):
        device = FakeDevice(SLOTS, initial_layout())
        kwargs = dict(
            driver=device,
            slots=SLOTS,
            eligible=("C",),
            active=OLD,
            target=NEW,
        )
        kwargs.update(overrides)
        return device, Rollout(**kwargs)

    def test_invalid_configurations_rejected_without_programming(self):
        cases = [
            dict(eligible=("Z",)),
            dict(eligible=("A", "A")),
            dict(slots={"A": 1, "B": 1}),
            dict(slots={"A": 3, "B": 1, "C": 1}),
            dict(slots={"A": 0, "B": 1, "C": 1}),
            dict(slots={"A": True, "B": 1, "C": 1}),
            dict(active={RED: OLD[RED]}),
            dict(active={RED: OLD[RED], BLUE: OLD[BLUE], "green": ()}),
            dict(active={RED: (), BLUE: OLD[BLUE]}),
            dict(active={RED: OLD[RED] * 2, BLUE: OLD[BLUE]}),
            dict(active={RED: (("198.51.100.1/24", "sent", "x"),), BLUE: OLD[BLUE]}),
            dict(active={RED: (("198.51.100.0/24", "bogus", None),), BLUE: OLD[BLUE]}),
            dict(active={RED: (("198.51.100.0/24", "drop", "x"),), BLUE: OLD[BLUE]}),
            dict(active={RED: (("198.51.100.0/24", "sent", None),), BLUE: OLD[BLUE]}),
            dict(active={RED: (("198.51.100.0/24", "sent", ""),), BLUE: OLD[BLUE]}),
            dict(active={"": OLD[RED], BLUE: OLD[BLUE]}),
            dict(target={RED: NEW[RED], "green": NEW[BLUE]}),
            dict(active={RED: OLD[RED], BLUE: OLD[BLUE]},
                 target={RED: NEW[RED], BLUE: NEW[BLUE], "green": ()}),
            dict(eligible="C"),
        ]
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    self.make(**case)

    def test_resolve_never_uses_memory_or_partial_images(self):
        device = FakeDevice(SLOTS, initial_layout())
        # Only the empty C is eligible: the old rows exist in memory and on the
        # ineligible A/B, but no eligible device holds them.
        rollout = Rollout(device, SLOTS, ("C",), OLD, NEW)
        with self.assertRaises(NoReadyImage):
            rollout.resolve(RED, "198.51.100.7")
        self.assertEqual(device.programs, [])

        # A half image plus an empty device does not become ready either.
        half = FakeDevice(
            SLOTS, {"A": [physical(OLD[RED][0], RED)], "B": [], "C": []}
        )
        rollout = Rollout(half, SLOTS, ("A", "B", "C"), OLD, NEW)
        with self.assertRaises(NoReadyImage):
            rollout.resolve(BLUE, "203.0.113.7")

        # A device with an extra row from the other revision is not ready for
        # the group, even though this address would answer the same.
        extra = FakeDevice(
            SLOTS,
            {
                "A": [physical(OLD[RED][0], RED), physical(NEW[RED][0], RED)],
                "B": [],
                "C": [],
            },
        )
        rollout = Rollout(extra, SLOTS, ("A", "B", "C"), OLD, NEW)
        with self.assertRaises(NoReadyImage):
            rollout.resolve(RED, "198.51.100.7")

    def test_unknown_group_and_bad_address(self):
        device, rollout = self.make()
        with self.assertRaises(ValueError):
            rollout.resolve("green", "1.2.3.4")
        with self.assertRaises(ValueError):
            rollout.resolve(RED, "1.2.3.999")
        with self.assertRaises(ValueError):
            rollout.resolve(RED, "198.51.100.0/24")


if __name__ == "__main__":
    unittest.main()
