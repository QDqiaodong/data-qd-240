"""Offline, slot-bounded rollout of reviewed lookup images.

A :class:`Rollout` moves two groups of reviewed lookup rows from one published
revision to the next on a tiny bank of simulated devices.  Each device has one
or two physical slots and each slot holds either ``None`` or exactly one
``(group, CIDR, action, egress)`` row.  Programming is done one slot at a time
through a caller-provided local driver; the reader-facing :meth:`Rollout.resolve`
only ever answers from a device whose physical rows for the requested group
exactly match the currently published revision.

The important safety rule is that every intermediate physical layout must still
serve *both* groups at revision 1, and revision 2 may only be switched on once
the same layout also contains complete revision-2 images for both groups.  A
layout with no such write order is rejected with :class:`NoSafeLayout` before a
single slot is touched.

All inputs are synthetic.  This module opens no sockets and talks to no real
device; it only calls the small driver object supplied by the caller.
"""

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from ipaddress import IPv4Address, IPv4Network

__all__ = [
    "NoReadyImage",
    "NoSafeLayout",
    "Rollout",
    "RolloutError",
]


MIN_DEVICES = 2
MAX_DEVICES = 3
MIN_SLOTS_PER_DEVICE = 1
MAX_SLOTS_PER_DEVICE = 2
MIN_ROWS_PER_GROUP = 1
MAX_ROWS_PER_GROUP = 2
ACTIONS = frozenset({"sent", "drop", "unrouted", "punt"})


class RolloutError(RuntimeError):
    """Base class for rollout-specific failures."""


class NoSafeLayout(RolloutError):
    """No slot-write order can place both target images safely.

    Raised while planning, before any physical slot has been programmed.
    """


class NoReadyImage(RolloutError):
    """No eligible device holds the complete image for the published revision."""


def _check_name(value, what):
    if type(value) is not str or not value:
        raise ValueError(f"{what} must be a non-empty string")
    return value


def _check_row(row, what="lookup row"):
    """Validate and normalize one ``(CIDR, action, egress)`` triple."""
    if isinstance(row, (str, bytes)) or not isinstance(row, Sequence):
        raise ValueError(f"{what} must be a (CIDR, action, egress) triple")
    if len(row) != 3:
        raise ValueError(f"{what} must contain exactly three items")
    cidr, action, egress = row

    if type(cidr) is not str:
        raise ValueError(f"{what} CIDR must be IPv4 prefix text")
    try:
        network = IPv4Network(cidr, strict=True)
    except ValueError as exc:
        raise ValueError(
            f"{what} requires a strict IPv4 CIDR prefix, got {cidr!r}"
        ) from exc
    if str(network) != cidr:
        raise ValueError(f"{what} CIDR is not canonical: {cidr!r}")

    if type(action) is not str or action not in ACTIONS:
        raise ValueError(f"{what} has unsupported action {action!r}")
    if action == "sent":
        if type(egress) is not str or not egress:
            raise ValueError(f"{what} action 'sent' requires a non-empty egress")
    elif egress is not None:
        raise ValueError(f"{what} action {action!r} requires egress None")

    return (cidr, action, egress)


def _parse_images(images, label):
    if not isinstance(images, Mapping):
        raise ValueError(f"{label} must be a mapping of group name to rows")
    if len(images) != 2:
        raise ValueError(f"{label} must contain exactly two groups")

    parsed = {}
    for group, raw_rows in images.items():
        _check_name(group, f"{label} group name")
        if isinstance(raw_rows, (str, bytes)) or not isinstance(
            raw_rows, Iterable
        ):
            raise ValueError(
                f"{label}[{group!r}] must be an iterable of lookup rows"
            )
        rows = tuple(raw_rows)
        if not MIN_ROWS_PER_GROUP <= len(rows) <= MAX_ROWS_PER_GROUP:
            raise ValueError(
                f"{label}[{group!r}] must contain 1 or 2 rows, "
                f"got {len(rows)}"
            )

        seen = set()
        normalized = []
        for row in rows:
            cidr, action, egress = _check_row(
                row, f"{label}[{group!r}] row"
            )
            network = IPv4Network(cidr, strict=True)
            if network in seen:
                raise ValueError(
                    f"{label}[{group!r}] has duplicate prefix {cidr}"
                )
            seen.add(network)
            normalized.append((cidr, action, egress))
        parsed[group] = tuple(normalized)

    return parsed


def _normalize_slot_rows(rows, capacity, groups, allowed_rows):
    """Validate one device snapshot against the fixed slot capacity."""
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise ValueError("inspect() must return a sequence of slot contents")
    if len(rows) != capacity:
        raise ValueError(
            f"inspect() returned {len(rows)} slots; device has {capacity}"
        )

    normalized = []
    seen = set()
    for row in rows:
        if row is None:
            normalized.append(None)
            continue
        if (
            isinstance(row, (str, bytes))
            or not isinstance(row, Sequence)
            or len(row) != 4
        ):
            raise ValueError(
                "each occupied slot must hold (group, CIDR, action, egress)"
            )
        group, cidr, action, egress = row
        _check_name(group, "slot row group")
        if group not in groups:
            raise ValueError(f"slot row uses unknown group {group!r}")
        cidr, action, egress = _check_row(
            (cidr, action, egress), f"slot row for group {group!r}"
        )
        physical = (group, cidr, action, egress)
        if physical not in allowed_rows:
            raise ValueError(
                "slot row is not part of the active or target image: "
                f"{physical!r}"
            )
        key = (group, cidr)
        if key in seen:
            raise ValueError(
                f"device repeats group/prefix {group!r} {cidr}"
            )
        seen.add(key)
        normalized.append(physical)

    return tuple(normalized)


class Rollout:
    """Plan and perform one safe two-group image rollout.

    :param driver: local simulated device driver with ``inspect(bank)``,
        ``program(bank, index, row)`` and ``lookup(bank, group, address)``.
    :param slots: mapping of device name to fixed slot count (2 or 3 devices,
        1 or 2 slots each).
    :param eligible: names of the devices usable for this drill.  Other devices
        are never read or written and do not count as capacity.
    :param active: mapping of the two group names to their revision-1 rows.
    :param target: mapping of the same two group names to their revision-2 rows.
    """

    def __init__(self, driver, slots, eligible, active, target):
        for method in ("inspect", "program", "lookup"):
            if not callable(getattr(driver, method, None)):
                raise ValueError(f"driver must provide a callable {method}()")
        self._driver = driver

        if not isinstance(slots, Mapping):
            raise ValueError("slots must be a mapping of device to slot count")
        if not MIN_DEVICES <= len(slots) <= MAX_DEVICES:
            raise ValueError("slots must contain 2 or 3 devices")
        parsed_slots = {}
        for name, capacity in slots.items():
            _check_name(name, "device name")
            if (
                isinstance(capacity, bool)
                or not isinstance(capacity, int)
                or not MIN_SLOTS_PER_DEVICE <= capacity <= MAX_SLOTS_PER_DEVICE
            ):
                raise ValueError(
                    f"device {name!r} must have 1 or 2 slots"
                )
            parsed_slots[name] = capacity
        self._slots = parsed_slots
        self._devices = tuple(parsed_slots)

        if isinstance(eligible, (str, bytes)) or not isinstance(
            eligible, Iterable
        ):
            raise ValueError("eligible must be an iterable of device names")
        eligible_names = tuple(eligible)
        selected = set()
        for name in eligible_names:
            _check_name(name, "eligible device name")
            if name not in parsed_slots:
                raise ValueError(f"eligible device {name!r} is not configured")
            if name in selected:
                raise ValueError(f"eligible device {name!r} is repeated")
            selected.add(name)
        # Keep the configured device order; eligibility is a set.
        self._eligible = tuple(
            name for name in self._devices if name in selected
        )

        self._active = _parse_images(active, "active")
        self._target = _parse_images(target, "target")
        if set(self._active) != set(self._target):
            raise ValueError(
                "active and target must describe the same two groups"
            )
        self._groups = tuple(sorted(self._active))

        self._active_rows = frozenset(
            (group, cidr, action, egress)
            for group, rows in self._active.items()
            for cidr, action, egress in rows
        )
        self._target_rows = frozenset(
            (group, cidr, action, egress)
            for group, rows in self._target.items()
            for cidr, action, egress in rows
        )
        self._allowed_rows = self._active_rows | self._target_rows

        self._revision = 1

    # ------------------------------------------------------------------
    # Read side
    # ------------------------------------------------------------------
    @property
    def revision(self):
        """Currently published revision: 1 before a successful publish."""
        return self._revision

    def resolve(self, group, address):
        """Read ``(revision, bank, action, egress)`` from a real device.

        Selection requires the device's physical rows for ``group`` to be exactly
        the published group image.  A device with a missing row, an extra row
        from the other revision, or merely the same answer for this particular
        address is not considered ready.  The action itself always comes from
        ``driver.lookup``; in-memory expected rows are never used to answer.
        """
        _check_name(group, "group")
        if group not in self._active:
            raise ValueError(f"unknown group {group!r}")
        if type(address) is not str:
            raise ValueError("address must be IPv4 prefix text")
        try:
            IPv4Address(address)
        except ValueError as exc:
            raise ValueError(f"invalid IPv4 address: {address!r}") from exc

        # Retry if a concurrent switch changes the published revision between
        # observing it and finding a ready device.  Physical rows are never
        # changed by the switch itself.
        for _ in range(4):
            revision = self._revision
            expected = frozenset(
                self._active[group]
                if revision == 1
                else self._target[group]
            )
            for bank in self._eligible:
                try:
                    raw = self._driver.inspect(bank)
                except OSError:
                    # An unavailable inspector cannot prove that this device
                    # still holds an intact image, so it may not serve reads.
                    continue
                rows = _normalize_slot_rows(
                    raw, self._slots[bank], self._groups, self._allowed_rows
                )
                if self._group_image(rows, group) != expected:
                    continue
                action, egress = self._driver.lookup(bank, group, address)
                if self._revision != revision:
                    break  # switched underneath us; re-resolve
                return (revision, bank, action, egress)
            if self._revision == revision:
                break

        raise NoReadyImage(
            f"no eligible device holds the complete revision-{revision} "
            f"image for group {group!r}"
        )

    # ------------------------------------------------------------------
    # Publish side
    # ------------------------------------------------------------------
    def publish(self):
        """Publish the target image, returning revision ``2`` on success.

        The whole write order is planned before the first slot is programmed.
        If no safe order exists, :class:`NoSafeLayout` is raised and no slot is
        touched.  If a ``program`` call raises ``OSError`` that exception is
        propagated unchanged; no further writes are attempted and the revision
        stays at 1.  ``publish`` may be called again on the same object once the
        driver is healthy, because every attempt starts from freshly inspected
        physical contents.
        """
        if self._revision == 2:
            return 2

        state = self._read_state()
        if state is None:
            # A device may be temporarily uninspectable.  With fewer usable
            # devices there is no safe order we can prove, so refuse without
            # touching anything; a retry re-reads from physical contents.
            raise NoSafeLayout(
                "a device cannot be inspected; refusing to program any slot"
            )
        if not self._serves(state, 1):
            raise NoSafeLayout(
                "no eligible device holds complete revision-1 images for both "
                "groups; refusing to program any slot"
            )

        plan = self._find_plan(state)
        if plan is None:
            raise NoSafeLayout(
                "no slot write order keeps both revision-1 images intact while "
                "placing complete revision-2 images for both groups"
            )

        for bank, index, row in plan:
            # OSError is intentionally not caught: a failed write may or may
            # not have taken effect, so we stop and let the caller retry from
            # freshly inspected contents.
            self._driver.program(bank, index, row)

        final = self._read_state()
        if final is None or not (self._serves(final, 1) and self._serves(final, 2)):
            raise RolloutError(
                "physical layout no longer satisfies the safe switch "
                "precondition after programming"
            )

        # The switch is the last step and is a single atomic revision change.
        self._revision = 2
        return 2

    def _read_state(self):
        state = []
        for bank in self._eligible:
            try:
                raw = self._driver.inspect(bank)
            except OSError:
                return None
            state.append(
                _normalize_slot_rows(
                    raw, self._slots[bank], self._groups, self._allowed_rows
                )
            )
        return tuple(state)

    @staticmethod
    def _group_image(rows, group):
        return frozenset(
            row[1:] for row in rows if row is not None and row[0] == group
        )

    def _serves(self, state, revision):
        images = self._active if revision == 1 else self._target
        for group in self._groups:
            expected = frozenset(images[group])
            if not any(
                self._group_image(rows, group) == expected for rows in state
            ):
                return False
        return True

    def _can_switch(self, state):
        return self._serves(state, 1) and self._serves(state, 2)

    def _find_plan(self, state):
        """Return the shortest safe write sequence, or ``None``.

        Breadth-first search over the complete physical layout of the eligible
        devices.  Every transition only writes a target row (or clears a slot)
        and is accepted only if both revision-1 group images remain complete
        afterwards.  Because the search is exhaustive, a layout is rejected
        only when *no* order works, not merely when the first arrangement tried
        fails.
        """
        if self._can_switch(state):
            return ()

        # A plan may copy an already-published physical row onto a still-free
        # slot of another device, so both revisions' rows are legal values.
        values = (
            tuple(sorted(self._active_rows, key=repr))
            + tuple(sorted(self._target_rows, key=repr))
            + (None,)
        )
        values = tuple(dict.fromkeys(values))
        queue = deque([state])
        parents = {state: None}
        moves = {}

        while queue:
            current = queue.popleft()
            seen_devices = set()
            for device_index, bank in enumerate(self._eligible):
                capacity = self._slots[bank]
                # Identical-capacity devices with identical contents are
                # interchangeable; exploring one of them is enough.
                device_key = (capacity, current[device_index])
                if device_key in seen_devices:
                    continue
                seen_devices.add(device_key)
                for index in range(capacity):
                    existing = current[device_index][index]
                    for value in values:
                        if value == existing:
                            continue
                        if value is not None and self._device_has(
                            current[device_index], value, skip=index
                        ):
                            continue
                        # Two copies of one physical row are enough to move it
                        # while keeping its group image available; more copies
                        # only grow the search.
                        if value is not None and self._count_row(
                            current, value
                        ) >= 2:
                            continue

                        slots = list(current[device_index])
                        slots[index] = value
                        candidate = (
                            current[:device_index]
                            + (tuple(slots),)
                            + current[device_index + 1:]
                        )
                        if candidate in parents:
                            continue
                        if not self._serves(candidate, 1):
                            continue

                        parents[candidate] = current
                        moves[candidate] = (bank, index, value)
                        if self._can_switch(candidate):
                            return self._reconstruct(
                                candidate, parents, moves
                            )
                        queue.append(candidate)

        return None

    @staticmethod
    def _count_row(state, row):
        return sum(1 for rows in state for other in rows if other == row)

    @staticmethod
    def _device_has(rows, row, skip):
        group, cidr = row[0], row[1]
        for index, other in enumerate(rows):
            if index == skip or other is None:
                continue
            if other[0] == group and other[1] == cidr:
                return True
        return False

    @staticmethod
    def _reconstruct(goal, parents, moves):
        plan = []
        node = goal
        while parents[node] is not None:
            plan.append(moves[node])
            node = parents[node]
        plan.reverse()
        return tuple(plan)
