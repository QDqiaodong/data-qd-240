"""Receive-side reassembly and in-order delivery of sliced UDP samples.

A :class:`WindowReceiver` is registered with a manifest of named ordered
streams.  Each stream independently reassembles fixed-length samples from
datagrams produced by :func:`encode_slice`, tolerating reordering, overlap and
duplication, and delivers complete samples to the comparator strictly in
sample-number order, one event per number.

The module never sends, retransmits or probes anything: :meth:`seal` is a local
scheduler decision, not a network control packet, and no thread, timer or
socket is used.  Only the Python 3.9+ standard library is required.

Pressure policy (one fixed choice, as required): **retain the predecessor**.
When accepting an incoming slice would leave more than ``byte_budget`` bytes
retained after the normal in-order delivery sweep, :meth:`feed` returns
``"pressure"`` with no events and does not retain any byte of the slice.  No
earlier sample is ever sacrificed; the caller may resubmit the same synthetic
slice after retained space has been released.
"""

import struct

__all__ = ["WindowReceiver", "encode_slice"]

_HEADER = struct.Struct(">II")
MAX_DATAGRAM = 4096
MAX_SAMPLE_LENGTH = 65536
MAX_STREAMS = 8
MAX_SAMPLES_PER_STREAM = 128
MAX_BUDGET = 1_048_576


def encode_slice(number, offset, data):
    """Encode one drill datagram: two big-endian uint32 fields then payload.

    The first uint32 is the one-based sample number, the second the zero-based
    byte offset inside that sample.  The manifest is not consulted here.
    ``bool`` is never accepted as an integer, sample number zero is rejected,
    the payload must be non-empty ``bytes``, the whole datagram must fit in the
    original 4096-byte send limit and ``offset + len(data)`` must not exceed the
    65536-byte maximum sample length.
    """
    if type(number) is bool or not isinstance(number, int):
        raise ValueError("sample number must be an integer")
    if not 1 <= number <= 0xFFFFFFFF:
        raise ValueError("sample number must be 1..2**32-1")
    if type(offset) is bool or not isinstance(offset, int):
        raise ValueError("offset must be an integer")
    if not 0 <= offset <= 0xFFFFFFFF:
        raise ValueError("offset must be 0..2**32-1")
    if type(data) is not bytes or len(data) == 0:
        raise ValueError("slice payload must be non-empty bytes")
    if offset + len(data) > MAX_SAMPLE_LENGTH:
        raise ValueError("slice runs past the 65536-byte sample boundary")
    datagram = _HEADER.pack(number, offset) + data
    if len(datagram) > MAX_DATAGRAM:
        raise ValueError("encoded datagram exceeds 4096 bytes")
    return datagram


def _decode_datagram(datagram):
    """Parse a received datagram into ``(number, offset, data)`` or raise."""
    if type(datagram) is not bytes or len(datagram) < _HEADER.size + 1:
        raise ValueError("malformed slice datagram")
    if len(datagram) > MAX_DATAGRAM:
        raise ValueError("slice datagram exceeds 4096 bytes")
    number, offset = _HEADER.unpack_from(datagram)
    if number == 0:
        raise ValueError("sample number zero is not valid")
    return number, offset, datagram[_HEADER.size:]


class _StreamState:
    """Reassembly state of one named ordered stream."""

    __slots__ = ("name", "lengths", "next_to_deliver", "samples", "created_order")

    def __init__(self, name, lengths):
        self.name = name
        # One-based sample number n has expected length lengths[n - 1].
        self.lengths = lengths
        self.next_to_deliver = 1
        # number -> {"data": bytearray, "have": bytearray of 0/1 flags}
        self.samples = {}
        self.created_order = []


class WindowReceiver:
    """Reassemble manifest-declared streams under one shared byte budget."""

    def __init__(self, manifest, byte_budget):
        if type(byte_budget) is bool or not isinstance(byte_budget, int):
            raise ValueError("byte_budget must be an integer, not bool")
        if not 0 <= byte_budget <= MAX_BUDGET:
            raise ValueError(f"byte_budget must be 0..{MAX_BUDGET}")
        if not isinstance(manifest, dict) or not manifest:
            raise ValueError("manifest must be a non-empty mapping")
        if len(manifest) > MAX_STREAMS:
            raise ValueError(f"at most {MAX_STREAMS} streams are supported")

        streams = {}
        for name, lengths in manifest.items():
            if type(name) is not str or not name:
                raise ValueError("receiver names must be non-empty strings")
            if isinstance(lengths, (str, bytes)):
                raise ValueError("sample length sequence must not be text/bytes")
            try:
                materialized = list(lengths)
            except TypeError as exc:
                raise ValueError(
                    "manifest value must be an iterable of sample lengths"
                ) from exc
            if not 1 <= len(materialized) <= MAX_SAMPLES_PER_STREAM:
                raise ValueError(
                    f"each stream must declare 1..{MAX_SAMPLES_PER_STREAM} samples"
                )
            for length in materialized:
                if type(length) is bool or not isinstance(length, int):
                    raise ValueError("sample lengths must be integers, not bool")
                if not 1 <= length <= MAX_SAMPLE_LENGTH:
                    raise ValueError(
                        f"sample lengths must be 1..{MAX_SAMPLE_LENGTH}"
                    )
            streams[name] = _StreamState(name, tuple(materialized))

        self._streams = streams
        self._budget = byte_budget
        # Distinct retained bytes of all not-yet-terminated samples.
        self._buffered = 0

    @property
    def buffered_bytes(self):
        """Read-only count of distinct bytes currently retained."""
        return self._buffered

    @staticmethod
    def _is_complete(sample, length):
        # Completeness is judged by actual byte coverage, never by slice
        # counts, accumulated lengths or zero filling.
        return sum(sample["have"]) == length

    def _leading_delivery_bytes(self, state):
        """Bytes that an in-order delivery sweep would free right now."""
        freed = 0
        for number in range(state.next_to_deliver, len(state.lengths) + 1):
            sample = state.samples.get(number)
            if sample is None or not self._is_complete(
                sample, state.lengths[number - 1]
            ):
                break
            freed += state.lengths[number - 1]
        return freed

    def _sweep(self, state):
        """Deliver every leading consecutive complete sample of one stream."""
        events = []
        while state.next_to_deliver <= len(state.lengths):
            number = state.next_to_deliver
            sample = state.samples.get(number)
            if sample is None or not self._is_complete(
                sample, state.lengths[number - 1]
            ):
                break
            data = bytes(sample["data"])
            del state.samples[number]
            state.created_order.remove(number)
            state.next_to_deliver += 1
            self._buffered -= len(data)
            events.append(("message", state.name, number, data))
        return events

    def feed(self, receiver, datagram):
        """Process one received datagram; return ``(status, events_tuple)``.

        Statuses: ``"accepted"``, ``"duplicate"``, ``"late"``, ``"conflict"``
        and ``"pressure"``.  Validation failures raise :class:`ValueError`
        before any state changes.  Only ``"accepted"`` can produce events.
        """
        state = self._streams.get(receiver)
        if state is None:
            raise ValueError(f"unknown receiver: {receiver!r}")

        number, offset, chunk = _decode_datagram(datagram)
        if not 1 <= number <= len(state.lengths):
            raise ValueError("sample number is not in this stream's manifest")
        length = state.lengths[number - 1]
        if offset + len(chunk) > length:
            raise ValueError("slice runs past the declared sample length")

        # A terminated number never reopens, even for a perfectly legal slice.
        if number < state.next_to_deliver:
            return "late", ()

        created = number not in state.samples
        if created:
            sample = {
                "data": bytearray(length),
                "have": bytearray(length),
            }
            state.samples[number] = sample
            state.created_order.append(number)
        else:
            sample = state.samples[number]

        new_positions = []
        for index, byte in enumerate(chunk):
            position = offset + index
            if sample["have"][position]:
                if sample["data"][position] != byte:
                    # Whole-slice rejection: even untouched new bytes of this
                    # datagram must not survive.
                    if created:
                        del state.samples[number]
                        state.created_order.remove(number)
                    return "conflict", ()
            else:
                new_positions.append((position, byte))

        if not new_positions:
            if created:
                del state.samples[number]
                state.created_order.remove(number)
            return "duplicate", ()

        # Apply tentatively so the delivery sweep can be evaluated exactly.
        for position, byte in new_positions:
            sample["data"][position] = byte
            sample["have"][position] = 1

        freed = self._leading_delivery_bytes(state)
        projected = self._buffered + len(new_positions) - freed
        if projected > self._budget:
            # Retain-predecessor policy: refuse the whole slice, keep every
            # earlier coverage and delivery decision untouched.
            for position, _byte in new_positions:
                sample["have"][position] = 0
                sample["data"][position] = 0
            if created:
                del state.samples[number]
                state.created_order.remove(number)
            return "pressure", ()

        self._buffered += len(new_positions)
        events = self._sweep(state)
        return "accepted", tuple(events)

    def seal(self, receiver, through):
        """Declare that this stream stops waiting for gaps up to ``through``.

        Returns the ordered events generated by this call: complete samples in
        the range are delivered as ``"message"`` events, incomplete ones
        (including never-received numbers) become permanent
        ``("gap", receiver, number, "sealed")`` events and lose their partial
        bytes.  Consecutive complete successors beyond ``through`` are delivered
        immediately.  Already terminated numbers never emit again.
        """
        state = self._streams.get(receiver)
        if state is None:
            raise ValueError(f"unknown receiver: {receiver!r}")
        if type(through) is bool or not isinstance(through, int):
            raise ValueError("seal bound must be an integer, not bool")
        if not 0 <= through <= len(state.lengths):
            raise ValueError(
                f"seal bound must be 0..{len(state.lengths)} for this stream"
            )

        events = []
        cursor = state.next_to_deliver
        for number in range(cursor, through + 1):
            sample = state.samples.pop(number, None)
            if sample is None:
                # Never received at all: an explicit permanent gap.
                events.append(("gap", receiver, number, "sealed"))
                continue
            if number in state.created_order:
                state.created_order.remove(number)
            retained = sum(sample["have"])
            self._buffered -= retained
            if retained == state.lengths[number - 1]:
                events.append(
                    ("message", receiver, number, bytes(sample["data"]))
                )
            else:
                # Partial bytes are discarded with the gap event.
                events.append(("gap", receiver, number, "sealed"))

        state.next_to_deliver = max(cursor, through + 1)
        events.extend(self._sweep(state))
        return tuple(events)
