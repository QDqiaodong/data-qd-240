"""A local, restartable delivery record for reassembled receive events.

One :class:`DeliverySpool` file belongs to one drill.  It records the
non-empty ``message``/``gap`` event groups produced by
:class:`receiving.WindowReceiver` and lets independent comparator readers
consume them at their own pace, across process restarts.  It never touches
the network, never recovers unfinished reassembly and never invents events:
the recorder hands it already-produced events.

Chosen acknowledgement/reclamation route: **independent commits**.  A
successful :meth:`DeliverySpool.confirm` persists the reader cursor on its
own.  :meth:`DeliverySpool.reclaim` is a separate explicit call that deletes
the common confirmed prefix.  If reclamation fails, the confirmation remains
valid and the unreclaimed events keep occupying capacity until a successful
retry.  See ``README.md`` for the complete contract.

Only the Python 3.9+ standard library is used.  The file format is private;
all access goes through this module.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass

__all__ = [
    "DeliverySpool",
    "DeliveryReceipt",
    "ReclaimResult",
    "DeliverySpoolError",
    "ConfigurationError",
    "RecordExists",
    "CorruptRecord",
    "UnknownReader",
    "InvalidPageSize",
    "InvalidAck",
    "InvalidEvent",
    "EmptyGroup",
    "GroupNumberError",
    "GroupConflict",
    "StaleGroup",
    "GroupGap",
    "CapacityExceeded",
    "NothingToReclaim",
    "SpoolWriteError",
]

_FORMAT = "delivery-spool"
_VERSION = 1
_MAX_SAMPLE_NUMBER = 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class DeliverySpoolError(Exception):
    """Base class for every delivery-record error."""


class ConfigurationError(DeliverySpoolError, ValueError):
    """Bad creation/opening configuration or a bad call argument."""


class RecordExists(ConfigurationError):
    """Creation was asked to reuse an existing path."""


class CorruptRecord(DeliverySpoolError, ValueError):
    """The local record does not match the expected format."""


class UnknownReader(DeliverySpoolError, LookupError):
    """The named reader is not part of this drill."""


class InvalidPageSize(ConfigurationError):
    """A read page size is not a positive non-bool integer."""


class InvalidAck(ConfigurationError):
    """A cumulative acknowledgement is not legal for this record."""


class InvalidEvent(ConfigurationError):
    """A submitted event does not have the receiver's shape."""


class EmptyGroup(InvalidEvent):
    """An empty event group is never submitted."""


class GroupNumberError(DeliverySpoolError, ValueError):
    """The supplied group number is not the expected one."""


class GroupConflict(GroupNumberError):
    """Same group number, different canonical content."""


class StaleGroup(GroupNumberError):
    """The group number is older than the latest committed group."""


class GroupGap(GroupNumberError):
    """The group number skips forward past the next expected one."""


class CapacityExceeded(DeliverySpoolError):
    """The retained-event budget cannot fit a new group.

    This is deliberately *not* a ``ValueError``/``GroupNumberError`` so that
    callers can distinguish a full record from malformed input and from
    ``NothingToReclaim``.  Nothing was changed and no number was consumed.
    """

    def __init__(self, *, requested, available, usage, capacity, group_number):
        self.requested = requested
        self.available = available
        self.usage = usage
        self.capacity = capacity
        self.group_number = group_number
        super().__init__(
            "capacity exceeded for group "
            f"{group_number}: requested {requested} bytes, "
            f"available {available} of {capacity} (usage {usage})"
        )


class NothingToReclaim(DeliverySpoolError):
    """There is no newly confirmed common prefix to delete.

    Distinct from both a full record and a failed reclamation write.
    """

    def __init__(self, *, reclaimed_through, cursors):
        self.reclaimed_through = reclaimed_through
        self.cursors = dict(cursors)
        super().__init__(
            "nothing to reclaim beyond delivery "
            f"{reclaimed_through} (confirmed {self.cursors})"
        )


class SpoolWriteError(DeliverySpoolError):
    """Persisting a record change failed; the change did not commit."""


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeliveryReceipt:
    """Immutable receipt for one committed group."""

    group_number: int
    first_delivery: int
    last_delivery: int
    event_count: int


@dataclass(frozen=True)
class ReclaimResult:
    """What one successful reclamation actually removed."""

    reclaimed_through: int
    freed_events: int
    freed_bytes: int
    usage: int


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_capacity(capacity):
    if not _is_int(capacity) or capacity <= 0:
        raise ConfigurationError("capacity must be a positive integer, not bool")
    return capacity


def _validate_readers(readers):
    if isinstance(readers, (str, bytes)):
        raise ConfigurationError("readers must be an iterable of names")
    try:
        materialized = list(readers)
    except TypeError as exc:
        raise ConfigurationError(
            "readers must be an iterable of names"
        ) from exc
    if not materialized:
        raise ConfigurationError("at least one reader name is required")
    seen = set()
    for name in materialized:
        if type(name) is not str or not name:
            raise ConfigurationError("reader names must be non-empty strings")
        if name in seen:
            raise ConfigurationError("reader names must be distinct")
        seen.add(name)
    return tuple(materialized)


def _validate_group_number(group_number):
    if not _is_int(group_number) or group_number < 1:
        raise GroupNumberError(
            "group number must be a positive integer, not bool"
        )
    return group_number


def _normalize_events(events):
    if isinstance(events, (str, bytes)):
        raise InvalidEvent("events must be an iterable of 4-tuples")
    try:
        materialized = tuple(events)
    except TypeError as exc:
        raise InvalidEvent(
            "events must be an iterable of 4-tuples"
        ) from exc
    if not materialized:
        raise EmptyGroup("an empty event group is never submitted")

    normalized = []
    for index, item in enumerate(materialized):
        if not isinstance(item, (tuple, list)) or len(item) != 4:
            raise InvalidEvent(f"event {index} must be a 4-tuple")
        kind, stream, number, body = item
        if type(kind) is not str or kind not in ("message", "gap"):
            raise InvalidEvent(
                f"event {index} kind must be 'message' or 'gap'"
            )
        if type(stream) is not str or not stream:
            raise InvalidEvent(
                f"event {index} stream must be a non-empty string"
            )
        if (
            not _is_int(number)
            or not 1 <= number <= _MAX_SAMPLE_NUMBER
        ):
            raise InvalidEvent(
                f"event {index} sample number must be 1..2**32-1"
            )
        if kind == "message":
            if type(body) is not bytes or not body:
                raise InvalidEvent(
                    f"event {index} payload must be non-empty bytes"
                )
            normalized.append(("message", stream, number, body))
        else:
            if type(body) is not str or not body:
                raise InvalidEvent(
                    f"event {index} gap reason must be a non-empty string"
                )
            normalized.append(("gap", stream, number, body))
    return tuple(normalized)


def _event_size(event):
    return 1 if event[0] == "gap" else len(event[3])


def _stored_size(event):
    return 1 if event["kind"] == "gap" else len(event["data"])


def _group_size(events):
    return sum(_event_size(event) for event in events)


def _canonical_digest(events):
    canonical = []
    for kind, stream, number, body in events:
        if kind == "message":
            canonical.append(
                ["message", stream, number, None,
                 base64.b64encode(body).decode("ascii")]
            )
        else:
            canonical.append(["gap", stream, number, body, None])
    raw = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# ---------------------------------------------------------------------------
# Private persistence seam
# ---------------------------------------------------------------------------


def _atomic_replace(source, destination):
    """The real commit boundary; tests may inject a failure here."""
    os.replace(source, destination)


def _after_commit(path):
    """Called after a record change is already durable.

    It is a no-op in normal use.  Tests may replace it to model a committed
    write whose caller never receives the receipt.
    """


def _fsync_directory(directory):
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _encode_event(event):
    if event["kind"] == "message":
        return {
            "delivery": event["delivery"],
            "kind": "message",
            "stream": event["stream"],
            "number": event["number"],
            "data": base64.b64encode(event["data"]).decode("ascii"),
        }
    return {
        "delivery": event["delivery"],
        "kind": "gap",
        "stream": event["stream"],
        "number": event["number"],
        "reason": event["reason"],
    }


def _decode_event(record, index):
    if not isinstance(record, dict):
        raise CorruptRecord(f"event {index} is not an object")
    delivery = record.get("delivery")
    if not _is_int(delivery) or delivery < 1:
        raise CorruptRecord(f"event {index} has a bad delivery number")
    kind = record.get("kind")
    stream = record.get("stream")
    number = record.get("number")
    if type(stream) is not str or not stream:
        raise CorruptRecord(f"event {index} has a bad stream")
    if not _is_int(number) or not 1 <= number <= _MAX_SAMPLE_NUMBER:
        raise CorruptRecord(f"event {index} has a bad sample number")
    if kind == "message":
        data = record.get("data")
        if type(data) is not str or not data:
            raise CorruptRecord(f"event {index} has bad payload text")
        try:
            payload = base64.b64decode(data.encode("ascii"), validate=True)
        except Exception as exc:
            raise CorruptRecord(
                f"event {index} payload is not base64"
            ) from exc
        if not payload:
            raise CorruptRecord(f"event {index} has an empty payload")
        return {
            "delivery": delivery,
            "kind": "message",
            "stream": stream,
            "number": number,
            "data": payload,
        }
    if kind == "gap":
        reason = record.get("reason")
        if type(reason) is not str or not reason:
            raise CorruptRecord(f"event {index} has a bad gap reason")
        return {
            "delivery": delivery,
            "kind": "gap",
            "stream": stream,
            "number": number,
            "reason": reason,
        }
    raise CorruptRecord(f"event {index} has an unknown kind")


def _state_to_document(state):
    return {
        "format": _FORMAT,
        "version": _VERSION,
        "capacity": state["capacity"],
        "readers": list(state["readers"]),
        "groups": [
            {
                "number": group["number"],
                "first": group["first"],
                "last": group["last"],
                "count": group["count"],
                "digest": group["digest"],
            }
            for group in state["groups"]
        ],
        "events": [_encode_event(event) for event in state["events"]],
        "cursors": {
            reader: state["cursors"][reader] for reader in state["readers"]
        },
        "next_delivery": state["next_delivery"],
        "reclaimed_through": state["reclaimed_through"],
    }


def _write_state(path, state):
    document = _state_to_document(state)
    payload = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, temporary = tempfile.mkstemp(
        prefix=".delivery-spool-", suffix=".tmp", dir=directory
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _atomic_replace(temporary, path)
        temporary = None
        _fsync_directory(directory)
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _new_state(readers, capacity):
    return {
        "capacity": capacity,
        "readers": tuple(readers),
        "groups": [],
        "events": [],
        "cursors": {reader: 0 for reader in readers},
        "next_delivery": 1,
        "reclaimed_through": 0,
    }


def _decode_state(document):
    if not isinstance(document, dict):
        raise CorruptRecord("record root is not an object")
    if document.get("format") != _FORMAT:
        raise CorruptRecord("record format marker is missing")
    if document.get("version") != _VERSION:
        raise CorruptRecord("unsupported record version")

    capacity = document.get("capacity")
    if not _is_int(capacity) or capacity <= 0:
        raise CorruptRecord("record capacity is invalid")

    readers = document.get("readers")
    try:
        readers = _validate_readers(readers)
    except ConfigurationError as exc:
        raise CorruptRecord(f"record readers are invalid: {exc}") from exc

    groups_in = document.get("groups")
    if not isinstance(groups_in, list):
        raise CorruptRecord("record groups are not a list")
    groups = []
    expected_number = 1
    expected_first = 1
    for index, group in enumerate(groups_in):
        if not isinstance(group, dict):
            raise CorruptRecord(f"group {index} is not an object")
        number = group.get("number")
        first = group.get("first")
        last = group.get("last")
        count = group.get("count")
        digest = group.get("digest")
        if not _is_int(number) or number != expected_number:
            raise CorruptRecord("group numbers are not contiguous from 1")
        if not _is_int(first) or first != expected_first:
            raise CorruptRecord("group delivery ranges are not contiguous")
        if not _is_int(count) or count < 1:
            raise CorruptRecord("group event count is invalid")
        if not _is_int(last) or last != first + count - 1:
            raise CorruptRecord("group delivery range is inconsistent")
        if type(digest) is not str or len(digest) != 64:
            raise CorruptRecord("group digest is invalid")
        try:
            int(digest, 16)
        except ValueError as exc:
            raise CorruptRecord("group digest is not hexadecimal") from exc
        groups.append(
            {
                "number": number,
                "first": first,
                "last": last,
                "count": count,
                "digest": digest,
            }
        )
        expected_number += 1
        expected_first = last + 1

    next_delivery = document.get("next_delivery")
    if not _is_int(next_delivery) or next_delivery < 1:
        raise CorruptRecord("next delivery number is invalid")
    if next_delivery != expected_first:
        raise CorruptRecord("next delivery number does not follow the groups")

    reclaimed_through = document.get("reclaimed_through")
    if (
        not _is_int(reclaimed_through)
        or reclaimed_through < 0
        or reclaimed_through > next_delivery - 1
    ):
        raise CorruptRecord("reclaimed delivery boundary is invalid")

    events_in = document.get("events")
    if not isinstance(events_in, list):
        raise CorruptRecord("record events are not a list")
    events = []
    expected_delivery = reclaimed_through + 1
    for index, record in enumerate(events_in):
        event = _decode_event(record, index)
        if event["delivery"] != expected_delivery:
            raise CorruptRecord("retained deliveries are not contiguous")
        if event["delivery"] > next_delivery - 1:
            raise CorruptRecord("retained delivery is beyond committed range")
        events.append(event)
        expected_delivery += 1
    if expected_delivery != next_delivery:
        raise CorruptRecord("retained events do not cover the committed tail")

    cursors_in = document.get("cursors")
    if not isinstance(cursors_in, dict) or set(cursors_in) != set(readers):
        raise CorruptRecord("record cursors do not match readers")
    cursors = {}
    for reader in readers:
        cursor = cursors_in[reader]
        if (
            not _is_int(cursor)
            or cursor < 0
            or cursor > next_delivery - 1
            or cursor < reclaimed_through
        ):
            raise CorruptRecord(f"cursor for {reader!r} is invalid")
        cursors[reader] = cursor

    return {
        "capacity": capacity,
        "readers": readers,
        "groups": groups,
        "events": events,
        "cursors": cursors,
        "next_delivery": next_delivery,
        "reclaimed_through": reclaimed_through,
    }


# ---------------------------------------------------------------------------
# Public record
# ---------------------------------------------------------------------------


class DeliverySpool:
    """One local, restartable record of committed receiver event groups."""

    def __init__(self, path, state):
        self._path = os.fspath(path)
        self._state = state

    # -- construction -----------------------------------------------------

    @classmethod
    def create(cls, path, readers, capacity):
        """Create a new local record; an existing path is never reused."""
        path = os.fspath(path)
        names = _validate_readers(readers)
        validated_capacity = _validate_capacity(capacity)
        if os.path.exists(path):
            raise RecordExists(f"record already exists: {path!r}")
        state = _new_state(names, validated_capacity)
        try:
            _write_state(path, state)
        except DeliverySpoolError:
            raise
        except Exception as exc:
            raise SpoolWriteError(
                f"failed to create delivery record {path!r}: {exc}"
            ) from exc
        return cls(path, state)

    @classmethod
    def open(cls, path):
        """Reopen an existing record, retaining its configuration/progress."""
        path = os.fspath(path)
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            raise
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptRecord("record is not valid JSON text") from exc
        return cls(path, _decode_state(document))

    # -- introspection ----------------------------------------------------

    @property
    def path(self):
        return self._path

    @property
    def readers(self):
        return tuple(self._state["readers"])

    @property
    def capacity(self):
        return self._state["capacity"]

    @property
    def usage(self):
        """Retained-event bytes: payload bytes for messages, 1 per gap."""
        return sum(_stored_size(event) for event in self._state["events"])

    @property
    def usage_bytes(self):
        """Alias for :attr:`usage`."""
        return self.usage

    @property
    def retained_events(self):
        return len(self._state["events"])

    @property
    def highest_delivery(self):
        """Highest delivery number ever committed (survives reclamation)."""
        return self._state["next_delivery"] - 1

    @property
    def reclaimed_through(self):
        """Highest delivery number already freed by reclamation."""
        return self._state["reclaimed_through"]

    @property
    def committed_groups(self):
        return len(self._state["groups"])

    @property
    def next_group_number(self):
        groups = self._state["groups"]
        return 1 if not groups else groups[-1]["number"] + 1

    def cursor(self, reader):
        """Return one reader's cumulative confirmed delivery number."""
        self._require_reader(reader)
        return self._state["cursors"][reader]

    def stored_receipt(self, group_number):
        """Return the durable receipt for a committed group number."""
        for group in self._state["groups"]:
            if group["number"] == group_number:
                return DeliveryReceipt(
                    group_number=group["number"],
                    first_delivery=group["first"],
                    last_delivery=group["last"],
                    event_count=group["count"],
                )
        return None

    # -- recording --------------------------------------------------------

    def submit(self, group_number, events):
        """Commit one non-empty group under the next consecutive number.

        The group number must be ``next_group_number`` or the latest already
        committed number.  Retrying the latest number with identical content
        returns its original receipt without touching the record, even when
        its events have been reclaimed or no capacity remains.  All failures
        leave the record byte-identical and consume no number.
        """
        group_number = _validate_group_number(group_number)
        normalized = _normalize_events(events)
        digest = _canonical_digest(normalized)
        state = self._state
        groups = state["groups"]

        if not groups:
            latest_number = 0
        else:
            latest_number = groups[-1]["number"]

        if group_number < latest_number:
            raise StaleGroup(
                f"group {group_number} is older than committed group "
                f"{latest_number}"
            )
        if group_number == latest_number:
            latest = groups[-1]
            if digest != latest["digest"]:
                raise GroupConflict(
                    f"group {group_number} was already committed with "
                    "different content"
                )
            return DeliveryReceipt(
                group_number=latest["number"],
                first_delivery=latest["first"],
                last_delivery=latest["last"],
                event_count=latest["count"],
            )
        if group_number > latest_number + 1:
            raise GroupGap(
                f"group {group_number} skips ahead; expected "
                f"{latest_number + 1}"
            )

        size = _group_size(normalized)
        usage = self.usage
        available = state["capacity"] - usage
        if size > available:
            raise CapacityExceeded(
                requested=size,
                available=available,
                usage=usage,
                capacity=state["capacity"],
                group_number=group_number,
            )

        new_state = copy.deepcopy(state)
        first = new_state["next_delivery"]
        delivery = first
        for kind, stream, number, body in normalized:
            if kind == "message":
                event = {
                    "delivery": delivery,
                    "kind": "message",
                    "stream": stream,
                    "number": number,
                    "data": body,
                }
            else:
                event = {
                    "delivery": delivery,
                    "kind": "gap",
                    "stream": stream,
                    "number": number,
                    "reason": body,
                }
            new_state["events"].append(event)
            delivery += 1
        last = delivery - 1
        new_state["next_delivery"] = delivery
        new_state["groups"].append(
            {
                "number": group_number,
                "first": first,
                "last": last,
                "count": len(normalized),
                "digest": digest,
            }
        )
        self._commit(new_state)
        return DeliveryReceipt(
            group_number=group_number,
            first_delivery=first,
            last_delivery=last,
            event_count=len(normalized),
        )

    # -- reading ----------------------------------------------------------

    def read(self, reader, limit):
        """Return up to ``limit`` retained events after the reader's cursor.

        Each item is ``(delivery_number, original_event_tuple)``.  Reading is
        not confirmation and may be repeated.
        """
        self._require_reader(reader)
        if not _is_int(limit) or limit <= 0:
            raise InvalidPageSize("page size must be a positive integer, not bool")
        cursor = self._state["cursors"][reader]
        page = []
        for event in self._state["events"]:
            delivery = event["delivery"]
            if delivery <= cursor:
                continue
            if event["kind"] == "message":
                original = (
                    "message",
                    event["stream"],
                    event["number"],
                    event["data"],
                )
            else:
                original = (
                    "gap",
                    event["stream"],
                    event["number"],
                    event["reason"],
                )
            page.append((delivery, original))
            if len(page) == limit:
                break
        return page

    # -- acknowledgement and reclamation ----------------------------------

    def confirm(self, reader, through):
        """Persist a cumulative confirmation, returning the current cursor.

        Re-confirming the current or an older number succeeds without a
        write.  The number may not exceed the highest committed delivery.
        """
        self._require_reader(reader)
        highest = self.highest_delivery
        if not _is_int(through) or not 0 <= through <= highest:
            raise InvalidAck(
                "confirmation must be an integer from 0 through "
                f"{highest}, not bool"
            )
        cursor = self._state["cursors"][reader]
        if through <= cursor:
            return cursor
        new_state = copy.deepcopy(self._state)
        new_state["cursors"][reader] = through
        self._commit(new_state)
        return through

    def reclaim(self):
        """Delete the common confirmed prefix and return its capacity.

        The reclamation is one committed record change.  If the write fails,
        no cursor moves and no byte is freed; the caller may retry only this
        call.  With no newly confirmed prefix, :class:`NothingToReclaim` is
        raised and nothing is changed.
        """
        state = self._state
        through = min(state["cursors"].values())
        base = state["reclaimed_through"]
        if through <= base:
            raise NothingToReclaim(
                reclaimed_through=base, cursors=state["cursors"]
            )
        removed = [
            event for event in state["events"] if event["delivery"] <= through
        ]
        if not removed:
            # Delivery numbers are contiguous, so this is a corrupt/invariant
            # breach rather than a normal "nothing to do".
            raise CorruptRecord("confirmed prefix has no retained events")
        freed_bytes = sum(_stored_size(event) for event in removed)
        new_state = copy.deepcopy(state)
        new_state["events"] = [
            event for event in state["events"] if event["delivery"] > through
        ]
        new_state["reclaimed_through"] = through
        self._commit(new_state)
        return ReclaimResult(
            reclaimed_through=through,
            freed_events=len(removed),
            freed_bytes=freed_bytes,
            usage=self.usage,
        )

    # -- internals --------------------------------------------------------

    def _require_reader(self, reader):
        if type(reader) is not str or reader not in self._state["cursors"]:
            raise UnknownReader(f"unknown reader: {reader!r}")

    def _commit(self, new_state):
        try:
            _write_state(self._path, new_state)
        except DeliverySpoolError:
            raise
        except Exception as exc:
            raise SpoolWriteError(
                f"failed to persist delivery record {self._path!r}: {exc}"
            ) from exc
        self._state = new_state
        _after_commit(self._path)
