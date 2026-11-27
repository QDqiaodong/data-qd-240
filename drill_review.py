"""In-process review spool with separate conclusion and raw-byte duties.

A :class:`ReviewSpool` consumes the offline terminal events produced by
:class:`receiving.WindowReceiver` (``message``/``gap`` tuples).  It never sends
packets, opens no socket and spawns no thread: the only way raw bytes can come
back after acceptance is the caller-supplied ``recall(stream, number)``
callable, which models an already-open local read-back source.

Two reading duties are kept independent:

* every registered reader pages through *conclusions* (delivery number,
  category, stream, sample number and, for gaps, the reason).  Conclusions
  hold no payload bytes;
* only the named ``reviewers`` may additionally re-read the exact original
  sample bytes through :meth:`ReviewSpool.sample`, and only while they have
  not confirmed that delivery.

Chosen placement policy (one fixed choice, as permitted): **greedy local
retention first**.  An accepted payload is held resident in memory when the
resident budget allows, otherwise in a private payload file inside the working
directory, otherwise it depends on read-back recall while a recall slot is
available.  Different identities may therefore use different tiers, and
:meth:`ReviewSpool.withdraw` may migrate stably recallable local samples to the
recall tier so that a source which must close can first be replaced by a local
copy.  Every persistent byte is counted in exactly one place: a sample never
has two persistent copies, and no compression, deduplication, extra file or
helper process is used to dodge the limits.  At most one sample may exist as a
transient working buffer during a single call; nothing is retained between
calls beyond the accounted holdings.

Only the Python 3.9+ standard library is used, calls are serial and no call
sleeps.
"""

from __future__ import annotations

import hashlib
import os
import tempfile

__all__ = [
    "ReviewSpool",
    "ReviewSpoolError",
    "ConfigurationError",
    "InvalidEvent",
    "InvalidAck",
    "InvalidPageSize",
    "UnknownReader",
    "CapacityError",
    "RecallError",
    "SpoolWriteError",
]

_MAX_READERS = 4
_MAX_REVIEWERS = 2
_MAX_BUDGET = 16 * 1024
_MAX_RECALL_SLOTS = 8
_MAX_DELIVERIES = 64
_MAX_SAMPLE_LENGTH = 4096
_MAX_SAMPLE_NUMBER = 0xFFFFFFFF
_PAYLOAD_PREFIX = "payload-"
_TEMP_PREFIX = ".review-"
_TEMP_SUFFIX = ".tmp"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ReviewSpoolError(Exception):
    """Base class for every review spool error."""


class ConfigurationError(ReviewSpoolError, ValueError):
    """Bad construction argument or bad call argument."""


class InvalidEvent(ConfigurationError):
    """A submitted event does not have the receiver terminal-event shape."""


class InvalidAck(ConfigurationError):
    """A confirmation value is not a legal cumulative delivery number."""


class InvalidPageSize(ConfigurationError):
    """A read page size is not a positive non-bool integer."""


class UnknownReader(ReviewSpoolError, LookupError):
    """The named reader is not part of this review spool."""


class CapacityError(ReviewSpoolError):
    """No tier can hold the payload under the configured budgets.

    The event is not admitted: no delivery number is consumed, the sample
    identity is not remembered and the caller may retry the same event once
    space has been released.
    """

    def __init__(self, *, length, resident_bytes, disk_bytes, recall_slots,
                 resident_used, disk_used, recall_used):
        self.length = length
        self.resident_bytes = resident_bytes
        self.disk_bytes = disk_bytes
        self.recall_slots = recall_slots
        self.resident_used = resident_used
        self.disk_used = disk_used
        self.recall_used = recall_used
        super().__init__(
            f"no capacity for a {length}-byte sample: resident "
            f"{resident_used}/{resident_bytes}, disk {disk_used}/"
            f"{disk_bytes}, recall {recall_used}/{recall_slots}"
        )


class RecallError(ReviewSpoolError):
    """Read-back did not reproduce the promised original bytes.

    Covers a source raising :class:`OSError`, returning non-bytes, returning an
    incomplete/wrong-length read or returning same-length different content.
    Promised conclusions and every reader cursor stay untouched, so the call
    may be retried after the fault is repaired.
    """


class SpoolWriteError(ReviewSpoolError):
    """A local payload-file write failed before any state changed."""


# ---------------------------------------------------------------------------
# Seams
# ---------------------------------------------------------------------------


def _write_payload_file(path, data):
    """Durably write ``data`` to ``path`` via a same-directory temp file.

    This is the single local write boundary; tests may replace it to model a
    disk failure.  A failure never leaves the temp file behind and never
    reaches ``os.replace``.
    """
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, temporary = tempfile.mkstemp(
        prefix=_TEMP_PREFIX, suffix=_TEMP_SUFFIX, dir=directory
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Internal records
# ---------------------------------------------------------------------------


class _Holding:
    """Where one accepted payload currently lives."""

    __slots__ = ("location", "length", "digest", "data", "path")

    def __init__(self, location, length, digest, data=None, path=None):
        # location is one of "resident", "disk", "recall".
        self.location = location
        self.length = length
        self.digest = digest
        self.data = data  # resident only
        self.path = path  # disk only


class _Entry:
    """One delivered conclusion, optionally still carrying its payload."""

    __slots__ = ("delivery", "kind", "stream", "number", "reason",
                 "holding", "identity")

    def __init__(self, delivery, kind, stream, number, reason, holding):
        self.delivery = delivery
        self.kind = kind
        self.stream = stream
        self.number = number
        self.reason = reason
        self.holding = holding
        self.identity = (stream, number)


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


# ---------------------------------------------------------------------------
# Public spool
# ---------------------------------------------------------------------------


class ReviewSpool:
    """Process-local spool separating conclusions from raw-byte review."""

    def __init__(self, directory, readers, reviewers, recall, *,
                 resident_bytes, disk_bytes, recall_slots):
        self._directory = self._validate_directory(directory)
        self._readers = self._validate_readers(readers)
        self._reviewers = self._validate_reviewers(reviewers, self._readers)
        if not callable(recall):
            raise ConfigurationError("recall must be callable")
        self._recall = recall
        self._resident_budget = self._validate_budget(
            resident_bytes, "resident_bytes"
        )
        self._disk_budget = self._validate_budget(disk_bytes, "disk_bytes")
        if not _is_int(recall_slots) or not 0 <= recall_slots <= _MAX_RECALL_SLOTS:
            raise ConfigurationError(
                f"recall_slots must be 0..{_MAX_RECALL_SLOTS}, not bool"
            )
        self._recall_slot_budget = recall_slots

        # delivery id (1-based) -> _Entry, kept in insertion order.
        self._entries = {}
        self._order = []
        # (stream, number) -> delivery id for every live entry.
        self._identities = {}
        # stream -> next expected per-stream sample number.
        self._stream_next = {}
        # identities whose read-back source has been closed with our consent.
        self._withdrawn = set()
        self._cursors = {reader: 0 for reader in self._readers}
        self._next_delivery = 1
        self._resident_used = 0
        self._disk_used = 0
        self._files = set()
        self._closed = False

    # -- construction validation -----------------------------------------

    @staticmethod
    def _validate_directory(directory):
        path = os.fspath(directory)
        if type(path) is not str or not path:
            raise ConfigurationError("directory must be a non-empty path")
        if not os.path.isdir(path):
            raise ConfigurationError(f"working directory missing: {path!r}")
        if os.listdir(path):
            raise ConfigurationError(
                f"working directory must be empty and exclusive: {path!r}"
            )
        return os.path.abspath(path)

    @staticmethod
    def _validate_readers(readers):
        if isinstance(readers, (str, bytes)):
            raise ConfigurationError("readers must be an iterable of names")
        try:
            materialized = tuple(readers)
        except TypeError as exc:
            raise ConfigurationError(
                "readers must be an iterable of names"
            ) from exc
        if not 1 <= len(materialized) <= _MAX_READERS:
            raise ConfigurationError(
                f"readers must contain 1..{_MAX_READERS} distinct names"
            )
        seen = set()
        for name in materialized:
            if type(name) is not str or not name:
                raise ConfigurationError(
                    "reader names must be non-empty strings"
                )
            if name in seen:
                raise ConfigurationError("reader names must be distinct")
            seen.add(name)
        return materialized

    @staticmethod
    def _validate_reviewers(reviewers, readers):
        if isinstance(reviewers, (str, bytes)):
            raise ConfigurationError("reviewers must be an iterable of names")
        try:
            materialized = tuple(reviewers)
        except TypeError as exc:
            raise ConfigurationError(
                "reviewers must be an iterable of names"
            ) from exc
        if not 1 <= len(materialized) <= _MAX_REVIEWERS:
            raise ConfigurationError(
                f"reviewers must contain 1..{_MAX_REVIEWERS} distinct names"
            )
        allowed = set(readers)
        seen = set()
        for name in materialized:
            if type(name) is not str or not name:
                raise ConfigurationError(
                    "reviewer names must be non-empty strings"
                )
            if name not in allowed:
                raise ConfigurationError(
                    f"reviewer {name!r} is not one of the readers"
                )
            if name in seen:
                raise ConfigurationError("reviewer names must be distinct")
            seen.add(name)
        return materialized

    @staticmethod
    def _validate_budget(value, label):
        if not _is_int(value) or not 0 <= value <= _MAX_BUDGET:
            raise ConfigurationError(
                f"{label} must be 0..{_MAX_BUDGET} bytes, not bool"
            )
        return value

    # -- introspection ----------------------------------------------------

    @property
    def readers(self):
        return tuple(self._readers)

    @property
    def reviewers(self):
        return tuple(self._reviewers)

    @property
    def highest_delivery(self):
        return self._next_delivery - 1

    @property
    def closed(self):
        return self._closed

    def cursor(self, reader):
        self._require_reader(reader)
        return self._cursors[reader]

    def usage(self):
        """Return ``(resident_payload_bytes, disk_payload_bytes, recall_ids)``.

        The recall tuple lists, in ascending delivery order, the distinct
        unconfirmed samples whose raw bytes are only available through their
        read-back source.
        """
        self._require_open()
        recall_ids = tuple(
            delivery for delivery in self._order
            if (entry := self._entries[delivery]).holding is not None
            and entry.holding.location == "recall"
        )
        return self._resident_used, self._disk_used, recall_ids

    # -- submission -------------------------------------------------------

    def submit(self, event):
        """Admit one receiver terminal event; return its delivery number.

        Delivery numbers are contiguous from one.  A gap is a valid event with
        a conclusion but no payload and consumes no byte budget.  A rejected
        message raises :class:`CapacityError`, consumes no number and remembers
        no identity, so the identical event can be retried later.
        """
        self._require_open()
        kind, stream, number, body = self._validate_event(event)

        if self._next_delivery > _MAX_DELIVERIES:
            raise CapacityError(
                length=0,
                resident_bytes=self._resident_budget,
                disk_bytes=self._disk_budget,
                recall_slots=self._recall_slot_budget,
                resident_used=self._resident_used,
                disk_used=self._disk_used,
                recall_used=self._recall_used(),
            )

        if kind == "gap":
            holding = None
        else:
            holding = self._place_payload(stream, number, body)

        delivery = self._next_delivery
        entry = _Entry(delivery, kind, stream, number,
                       body if kind == "gap" else None, holding)
        self._entries[delivery] = entry
        self._order.append(delivery)
        self._identities[(stream, number)] = delivery
        self._stream_next[stream] = number + 1
        self._next_delivery += 1
        return delivery

    def _validate_event(self, event):
        if not isinstance(event, (tuple, list)) or len(event) != 4:
            raise InvalidEvent("event must be a 4-tuple")
        kind, stream, number, body = event
        if kind not in ("message", "gap"):
            raise InvalidEvent("event kind must be 'message' or 'gap'")
        if type(stream) is not str or not stream:
            raise InvalidEvent("event stream must be a non-empty string")
        if not _is_int(number) or not 1 <= number <= _MAX_SAMPLE_NUMBER:
            raise InvalidEvent(
                f"sample number must be 1..{_MAX_SAMPLE_NUMBER}, not bool"
            )
        expected = self._stream_next.get(stream, 1)
        if number != expected:
            raise InvalidEvent(
                f"stream {stream!r} sample numbers must arrive in order: "
                f"expected {expected}, got {number}"
            )
        if kind == "message":
            if type(body) is not bytes or not body:
                raise InvalidEvent("message payload must be non-empty bytes")
            if not 1 <= len(body) <= _MAX_SAMPLE_LENGTH:
                raise InvalidEvent(
                    f"sample payload must be 1..{_MAX_SAMPLE_LENGTH} bytes"
                )
        else:
            if type(body) is not str or not body:
                raise InvalidEvent("gap reason must be a non-empty string")
        return kind, stream, number, body

    def _place_payload(self, stream, number, body):
        """Choose a tier for a brand-new payload without persistent copies."""
        length = len(body)
        if self._resident_budget - self._resident_used >= length:
            self._resident_used += length
            return _Holding("resident", length,
                            hashlib.sha256(body).digest(), data=body)
        if self._disk_budget - self._disk_used >= length:
            return self._write_new_disk_holding(stream, number, body, length)
        if self._recall_used() < self._recall_slot_budget:
            return _Holding("recall", length,
                            hashlib.sha256(body).digest())
        raise CapacityError(
            length=length,
            resident_bytes=self._resident_budget,
            disk_bytes=self._disk_budget,
            recall_slots=self._recall_slot_budget,
            resident_used=self._resident_used,
            disk_used=self._disk_used,
            recall_used=self._recall_used(),
        )

    def _write_new_disk_holding(self, stream, number, body, length):
        # The delivery id is not assigned yet; use the unique identity plus a
        # uniqueness counter guarded by call seriality.
        token = f"{self._next_delivery}"
        path = os.path.join(
            self._directory, f"{_PAYLOAD_PREFIX}{token}.bin"
        )
        try:
            _write_payload_file(path, body)
        except OSError as exc:
            raise SpoolWriteError(
                f"failed to persist payload for {stream!r} sample {number}: "
                f"{exc}"
            ) from exc
        self._files.add(path)
        self._disk_used += length
        return _Holding("disk", length, hashlib.sha256(body).digest(),
                        path=path)

    # -- conclusion reading and confirmation ------------------------------

    def read(self, reader, limit):
        """Page through conclusions after this reader's own cursor.

        Returns up to ``limit`` tuples
        ``(delivery_id, category, stream, sample_number, gap_reason_or_None)``
        in delivery order.  Reading never confirms and never advances a
        cursor; it may be repeated.
        """
        self._require_open()
        self._require_reader(reader)
        if not _is_int(limit) or limit <= 0:
            raise InvalidPageSize(
                "page size must be a positive integer, not bool"
            )
        cursor = self._cursors[reader]
        page = []
        for delivery in self._order:
            if delivery <= cursor:
                continue
            entry = self._entries[delivery]
            page.append((
                entry.delivery,
                entry.kind,
                entry.stream,
                entry.number,
                entry.reason,
            ))
            if len(page) == limit:
                break
        return page

    def confirm(self, reader, through):
        """Cumulatively confirm deliveries through ``through`` for a reader.

        Re-confirming the same or an older number is a side-effect-free
        success.  Confirmations may release payloads (once every reviewer has
        confirmed) and conclusions (once every reader has confirmed).
        """
        self._require_open()
        self._require_reader(reader)
        highest = self.highest_delivery
        if not _is_int(through) or not 0 <= through <= highest:
            raise InvalidAck(
                f"confirmation must be an integer from 0 through {highest}, "
                "not bool"
            )
        cursor = self._cursors[reader]
        if through <= cursor:
            return cursor
        self._cursors[reader] = through
        self._reclaim()
        return through

    def _reclaim(self):
        reviewer_floor = min(
            self._cursors[name] for name in self._reviewers
        )
        reader_floor = min(self._cursors.values())

        # Payloads become releasable once the last reviewer confirms; normal
        # readers that lag behind keep reading the payload-free conclusion.
        for delivery in self._order:
            entry = self._entries.get(delivery)
            if entry is None:
                continue
            holding = entry.holding
            if holding is not None and delivery <= reviewer_floor:
                self._release_holding(holding)
                entry.holding = None

        # Conclusions themselves disappear only after every reader confirms.
        for delivery in tuple(self._order):
            if delivery > reader_floor:
                break
            entry = self._entries.pop(delivery)
            self._order.remove(delivery)
            self._identities.pop(entry.identity, None)
            self._withdrawn.discard(entry.identity)
            if entry.holding is not None:
                # Defensive: payload release above should already have run.
                self._release_holding(entry.holding)
                entry.holding = None

    def _release_holding(self, holding):
        if holding.location == "resident":
            self._resident_used -= holding.length
            holding.data = None
        elif holding.location == "disk":
            path = holding.path
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                # Keep the accounting honest: the bytes may still occupy the
                # disk even if unlink failed, so leave them counted and keep
                # the path tracked for close()/retry rather than pretending
                # the space was freed.
                return
            self._files.discard(path)
            self._disk_used -= holding.length
            holding.path = None
        # recall holdings occupy neither local budget.

    # -- raw-byte review --------------------------------------------------

    def sample(self, reader, delivery_id):
        """Return the exact original bytes of one unconfirmed message.

        Only reviewers that have not confirmed ``delivery_id`` may call it.
        Unknown readers, confirmed deliveries, gaps and released entries raise
        :class:`LookupError`.  Any read-back failure (source error, truncated
        read or same-length different content) raises :class:`RecallError`
        without moving a cursor or weakening a promise.
        """
        self._require_open()
        if reader not in self._reviewers:
            raise LookupError(
                f"reader {reader!r} has no raw-byte review permission"
            )
        if not _is_int(delivery_id) or delivery_id <= 0:
            raise ConfigurationError("delivery id must be a positive integer")
        entry = self._entries.get(delivery_id)
        if entry is None or entry.kind == "gap":
            raise LookupError(
                f"delivery {delivery_id} has no reviewable sample"
            )
        if self._cursors[reader] >= delivery_id:
            raise LookupError(
                f"reader {reader!r} already confirmed delivery {delivery_id}"
            )
        holding = entry.holding
        if holding is None:
            # Defensive: an unconfirmed reviewer must always have a holding.
            raise LookupError(
                f"delivery {delivery_id} payload is no longer available"
            )

        if holding.location == "resident":
            payload = holding.data
            if payload is None or len(payload) != holding.length or \
                    hashlib.sha256(payload).digest() != holding.digest:
                raise RecallError("resident copy failed verification")
            return payload

        if holding.location == "disk":
            try:
                with open(holding.path, "rb") as handle:
                    payload = handle.read()
            except OSError as exc:
                raise RecallError(
                    f"local payload for delivery {delivery_id} is unreadable: "
                    f"{exc}"
                ) from exc
            self._verify(payload, holding, delivery_id)
            return payload

        if entry.identity in self._withdrawn:
            # A withdrawn identity must never be recalled again; reaching here
            # means an internal invariant was broken rather than a source fault.
            raise RecallError(
                f"source for {entry.identity!r} was already closed"
            )
        payload = self._read_source(entry.stream, entry.number)
        self._verify(payload, holding, delivery_id)
        return payload

    def _read_source(self, stream, number):
        try:
            raw = self._recall(stream, number)
        except OSError as exc:
            raise RecallError(
                f"recall source for {stream!r} sample {number} failed: {exc}"
            ) from exc
        except RecallError:
            raise
        except Exception as exc:
            raise RecallError(
                f"recall source for {stream!r} sample {number} raised "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if type(raw) is not bytes:
            raise RecallError(
                f"recall source for {stream!r} sample {number} returned "
                "non-bytes content"
            )
        return raw

    @staticmethod
    def _verify(payload, holding, delivery_id):
        if len(payload) != holding.length:
            raise RecallError(
                f"recall of delivery {delivery_id} returned "
                f"{len(payload)} bytes, expected {holding.length}"
            )
        if hashlib.sha256(payload).digest() != holding.digest:
            raise RecallError(
                f"recall of delivery {delivery_id} returned different content"
            )

    # -- source withdrawal ------------------------------------------------

    def withdraw(self, stream, number):
        """Mark one identity's read-back source as about to close.

        Returns ``True`` when no still-promised sample depends on that source:
        either the spool never relied on it, the reviewers already released the
        payload, or a verified local copy has just been made (possibly by
        demoting other stably recallable samples).  Returns ``False`` when the
        source cannot yet be closed safely; the caller must keep it open and
        retry later.  No conclusion, cursor or prior promise is changed by a
        ``False`` result.  Only this identity is affected.
        """
        self._require_open()
        if type(stream) is not str or not stream:
            raise ConfigurationError("stream must be a non-empty string")
        if not _is_int(number) or not 1 <= number <= _MAX_SAMPLE_NUMBER:
            raise ConfigurationError(
                f"sample number must be 1..{_MAX_SAMPLE_NUMBER}, not bool"
            )
        identity = (stream, number)
        if identity in self._withdrawn:
            return True
        delivery = self._identities.get(identity)
        if delivery is None:
            # Nothing of ours still names this identity.
            self._withdrawn.add(identity)
            return True
        entry = self._entries[delivery]
        holding = entry.holding
        if holding is None or holding.location != "recall":
            # Locally held (or already released): closing the source is safe.
            self._withdrawn.add(identity)
            return True

        # The promised bytes currently live only behind the source.  Probe it
        # before touching any state; a failed probe means "not yet closable".
        try:
            payload = self._read_source(stream, number)
            self._verify(payload, holding, delivery)
        except RecallError:
            return False

        tier, demotions = self._plan_migration(holding.length, entry)
        if tier is None:
            return False

        staged = None
        if tier == "disk":
            # Stage the replacement copy first as the single allowed in-call
            # working buffer.  A failure here happens before any state change,
            # so the old state is preserved byte-for-byte.
            fd, staged = tempfile.mkstemp(
                prefix=_TEMP_PREFIX, suffix=_TEMP_SUFFIX, dir=self._directory
            )
            os.close(fd)
            try:
                _write_payload_file(staged, payload)
            except OSError:
                try:
                    os.unlink(staged)
                except OSError:
                    pass
                return False

        # Demote verified local samples.  Their sources were probed and remain
        # open, so every demoted promise stays readable via recall even though
        # its local copy is gone; no conclusion or cursor is revoked.
        demotion_failed = False
        for demoted_entry in demotions:
            demoted_holding = demoted_entry.holding
            if demoted_holding.location == "disk":
                try:
                    os.unlink(demoted_holding.path)
                except OSError:
                    # Keep the local copy and its accounting; this demotion
                    # cannot complete, so the withdrawal stays uncommitted.
                    demotion_failed = True
                    break
                self._files.discard(demoted_holding.path)
                self._disk_used -= demoted_holding.length
                demoted_holding.path = None
            else:
                self._resident_used -= demoted_holding.length
                demoted_holding.data = None
            demoted_holding.location = "recall"

        if demotion_failed:
            if tier == "disk":
                try:
                    os.unlink(staged)
                except OSError:
                    pass
            return False

        if tier == "resident":
            # ``payload`` is the single allowed in-call working buffer.
            self._resident_used += holding.length
            holding.location = "resident"
            holding.data = payload
        else:
            path = os.path.join(
                self._directory, f"{_PAYLOAD_PREFIX}{delivery}.bin"
            )
            try:
                os.replace(staged, path)
            except OSError:
                try:
                    os.unlink(staged)
                except OSError:
                    pass
                # Demotions only moved samples onto probe-verified, still-open
                # sources: no promise was revoked; retry the withdrawal later.
                return False
            self._files.add(path)
            self._disk_used += holding.length
            holding.location = "disk"
            holding.path = path
        self._withdrawn.add(identity)
        return True

    def _plan_migration(self, needed, target_entry):
        """Find a local tier with room, demoting verified samples if needed.

        Returns ``(tier, entries_to_demote)`` or ``(None, ())`` when no safe
        plan exists.  Every demotion candidate is probed through its own
        source before it is accepted, so no plan relies on a broken source.
        Recall-slot budget is honoured for the demotions as well.
        """
        for tier, budget, used in (
            ("disk", self._disk_budget, self._disk_used),
            ("resident", self._resident_budget, self._resident_used),
        ):
            demotions = []
            room = budget - used
            if room >= needed:
                return tier, tuple()
            candidates = [
                self._entries[d] for d in self._order
                if d != target_entry.delivery
                and (entry := self._entries[d]).holding is not None
                and entry.holding.location == tier
                and entry.identity not in self._withdrawn
            ]
            for candidate in candidates:
                if self._recall_used() + len(demotions) >= \
                        self._recall_slot_budget:
                    break
                candidate_holding = candidate.holding
                try:
                    raw = self._read_source(candidate.stream,
                                            candidate.number)
                    self._verify(raw, candidate_holding, candidate.delivery)
                except RecallError:
                    continue
                demotions.append(candidate)
                room += candidate_holding.length
                if room >= needed:
                    return tier, tuple(demotions)
        return None, ()

    # -- lifecycle --------------------------------------------------------

    def close(self):
        """Remove every payload file this spool created; nothing else."""
        if self._closed:
            return
        for path in list(self._files):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        # Sweep any same-directory temp files bearing our marker; unrelated
        # files are never touched.
        try:
            names = os.listdir(self._directory)
        except OSError:
            names = ()
        for name in names:
            if name.startswith(_TEMP_PREFIX) and name.endswith(_TEMP_SUFFIX):
                try:
                    os.unlink(os.path.join(self._directory, name))
                except OSError:
                    pass
        self._files.clear()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    # -- small helpers ----------------------------------------------------

    def _recall_used(self):
        return sum(
            1 for delivery in self._order
            if (entry := self._entries[delivery]).holding is not None
            and entry.holding.location == "recall"
        )

    def _require_reader(self, reader):
        if type(reader) is not str or reader not in self._cursors:
            raise UnknownReader(f"unknown reader: {reader!r}")

    def _require_open(self):
        if self._closed:
            raise ReviewSpoolError("this review spool is already closed")
