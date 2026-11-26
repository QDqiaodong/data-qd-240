"""Userspace IPv4 destination policy with explicit loopback UDP egress.

Batch reservations are local metadata only.  A reservation records the longest
prefix and named egress selected at reservation time; later table replacements
may change the matched prefix but may not change the promised egress name.
Actual UDP datagrams are sent only by ``dispatch_next``.
"""

from array import array
from dataclasses import dataclass
from functools import lru_cache
from ipaddress import IPv4Address, IPv4Network
import socket
import threading
import uuid


MAX_BATCH_SIZE = 16
MAX_ACTIVE_SAMPLES = 64
# Short aliases for callers/tests using the specification's limit names.
MAX_BATCH = MAX_BATCH_SIZE
MAX_SAMPLES = MAX_ACTIVE_SAMPLES

# Offload compiler scale limits.
MAX_OFFLOAD_ROUTES = 4096
MAX_OFFLOAD_RANGES = 4096
MAX_OFFLOAD_EGRESSES = 32
MAX_OFFLOAD_ROWS = 8192


@dataclass(frozen=True)
class Outcome:
    action: str
    prefix: str = None
    egress: str = None


class ForwarderError(Exception):
    """Base class for errors specific to this forwarder."""


class BatchError(ForwarderError):
    """Base class for batch reservation/dispatch errors."""


class BatchRejected(BatchError, ValueError):
    """A whole reservation was rejected before registering any capacity.

    ``index`` is the zero-based offending sample position, or ``None`` for a
    batch-level rejection such as an empty or oversized batch.  ``reason`` is
    one of ``empty``, ``too_large``, ``invalid``, ``drop``, ``unrouted`` or
    ``capacity``.
    """

    def __init__(self, reason, message=None, *, index=None, prefix=None,
                 outcome=None, detail=None, failures=None, **extra):
        self.reason = reason
        self.kind = reason
        self.code = reason
        self.index = index
        self.position = index
        self.entry_index = index
        self.sample_index = index
        self.failed_index = index
        self.rejected_index = index
        self.offset = index
        self.loc = index
        self.at = index
        self.prefix = prefix
        self.outcome = outcome
        self.detail = detail
        self.failures = list(failures or ())
        for key, value in extra.items():
            setattr(self, key, value)
        if message is None:
            location = "batch" if index is None else f"sample {index}"
            message = f"batch rejected ({reason}) at {location}"
        super().__init__(message)


class EmptyBatch(BatchRejected):
    def __init__(self):
        super().__init__("empty", "cannot reserve an empty batch", index=None)


# Alternate exception names used by some API vocabularies.
BatchRejection = BatchRejected
ReserveRejected = BatchRejected
ReservationRejected = BatchRejected
InvalidBatch = BatchRejected


class InvalidBatchEntry(BatchRejected):
    def __init__(self, index, detail, *, failures=None):
        super().__init__(
            "invalid",
            f"invalid sample at index {index}: {detail}",
            index=index,
            detail=detail,
            failures=failures,
        )


# Alternate descriptive name.
InvalidSample = InvalidBatchEntry


class BatchRouteRejected(BatchRejected):
    def __init__(self, index, outcome):
        super().__init__(
            outcome.action,
            f"sample at index {index} is {outcome.action}",
            index=index,
            prefix=outcome.prefix,
            outcome=outcome,
        )


class CapacityError(BatchRejected):
    """The instance does not have enough unfinished-sample capacity."""

    def __init__(self, message=None, *, requested=None, available=None,
                 active=None, limit=MAX_ACTIVE_SAMPLES, reason="capacity"):
        self.requested = requested
        self.available = available
        self.active = active
        self.limit = limit
        super().__init__(
            reason,
            message or (
                f"batch needs {requested} slots but only {available} of "
                f"{limit} are available ({active} active)"
            ),
            index=None,
            requested=requested,
            available=available,
            active=active,
            limit=limit,
        )


# Common aliases so callers/tests can use the vocabulary from the design.
BatchCapacityError = CapacityError
BatchCapacityExceeded = CapacityError
CapacityExceeded = CapacityError
BatchFull = CapacityError


class BatchTooLarge(CapacityError):
    def __init__(self, requested, limit=MAX_BATCH_SIZE):
        super().__init__(
            f"batch contains {requested} samples; maximum is {limit}",
            requested=requested,
            available=limit,
            active=0,
            limit=limit,
            reason="too_large",
        )
        self.max_batch_size = limit


class BatchBusy(BatchError):
    """The ticket already has one claimed send that has not returned."""

    reason = "batch_busy"
    code = "batch_busy"


class UnknownTicket(LookupError):
    """The ticket was never issued here or has already ended."""


# Descriptive aliases.
InvalidTicket = UnknownTicket
UnknownBatchTicket = UnknownTicket
TicketNotFound = UnknownTicket


class RouteProtected(ForwarderError):
    """A replacement would break an egress promise for a managed sample."""

    def __init__(self, message, *, revision=None, conflicts=None):
        self.reason = "route_protected"
        self.code = self.reason
        self.revision = revision
        self.conflicts = list(conflicts or ())
        first = self.conflicts[0] if self.conflicts else {}
        self.conflict = first
        self.ticket = first.get("ticket")
        self.index = first.get("index")
        self.position = self.index
        self.promised_egress = first.get("promised_egress")
        self.promised_prefix = first.get("promised_prefix")
        super().__init__(message)


# Alternate name for callers that phrase the condition as a conflict.
RouteProtectionError = RouteProtected
RouteConflict = RouteProtected


class RevisionConflict(ForwarderError):
    """An optimistic replacement observed a newer revision."""


class OffloadBudgetExceeded(ForwarderError, ValueError):
    """No correct offload image fits within the requested row budget."""

    def __init__(self, message=None, *, budget=None, minimum_rows=None):
        self.budget = budget
        self.max_rows = budget
        self.minimum_rows = minimum_rows
        if message is None:
            message = (
                "no correct offload image fits in "
                f"{budget!r} rows (minimum={minimum_rows!r})"
            )
        super().__init__(message)


class SoftwareUnavailable(ForwarderError, RuntimeError):
    """A destination must use software, but software fallback is disabled."""


@dataclass(frozen=True)
class BatchTicket:
    """Opaque, process-unique ticket.  Do not rely on its fields."""

    _token: uuid.UUID
    _forwarder_id: int

    def __repr__(self):  # Keep the token opaque in ordinary output.
        return f"<BatchTicket ...{self._token.hex[-8:]}>"


# Alternate opaque name.
Ticket = BatchTicket


# Internal label numbers are also bit positions in small integer masks.
_LABEL_PUNT = 0
_LABEL_DROP = 1
_LABEL_UNROUTED = 2


def _parse_ipv4_text(destination):
    """Validate the public IPv4-text input and return an address object."""
    if type(destination) is not str:
        raise ValueError("destination must be IPv4 text")
    try:
        return IPv4Address(destination)
    except ValueError as exc:
        raise ValueError(f"invalid IPv4 destination: {destination!r}") from exc


def lookup_image(rows, destination):
    """Longest-prefix lookup using only exported image ``rows``.

    An unmatched address is explicitly represented as ``("punt", None)``.
    This function never consults a :class:`Forwarder` or its live table.
    """
    address = _parse_ipv4_text(destination)
    return _lookup_image_parsed(rows, address)


def _parse_offload_ranges(offload_ranges):
    """Validate and normalize the operator-supplied LPM boolean ranges."""
    try:
        iterator = iter(offload_ranges)
    except TypeError as exc:
        raise ValueError(
            "offload_ranges must be an iterable of (strict CIDR, bool) pairs"
        ) from exc

    parsed = []
    seen = set()
    for rule in iterator:
        if len(parsed) >= MAX_OFFLOAD_RANGES:
            raise ValueError(
                f"at most {MAX_OFFLOAD_RANGES} offload ranges are supported"
            )
        try:
            prefix_text, allowed = rule
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "each offload range must be a (strict CIDR text, bool) pair"
            ) from exc

        if type(prefix_text) is not str or type(allowed) is not bool:
            raise ValueError(
                "offload range prefix must be IPv4 CIDR text and value bool"
            )
        try:
            network = IPv4Network(prefix_text, strict=True)
        except ValueError as exc:
            raise ValueError(
                f"invalid strict IPv4 CIDR range: {prefix_text!r}"
            ) from exc
        if network in seen:
            raise ValueError(f"duplicate offload range prefix: {network}")

        seen.add(network)
        parsed.append((network, allowed))

    return parsed


def _build_offload_image(table, egress_names, range_rules, max_rows):
    """Build an exact-minimum LPM image for allowed parts of one table snapshot.

    The compressed binary trie DP uses two facts for every subtree:

    * ``cost[node]`` is the minimum sum when no row is installed at the node,
      minimized over the label of the nearest ancestor image row;
    * ``mask[node]`` is the non-empty set of ancestor labels attaining that
      minimum.

    For an ancestor label in the mask, the subtree costs ``cost``; otherwise
    installing one row at the node with a label from the mask gives
    ``cost + 1``.  This yields an exact global minimum without expanding IP
    ranges to individual addresses.
    """
    egress_names = tuple(egress_names)
    if len(egress_names) > MAX_OFFLOAD_EGRESSES:
        raise ValueError(
            f"at most {MAX_OFFLOAD_EGRESSES} egresses are supported"
        )

    label_info = [
        ("punt", None),
        ("drop", None),
        ("unrouted", None),
    ]
    egress_label = {}
    for index, name in enumerate(egress_names, start=_LABEL_UNROUTED + 1):
        egress_label[name] = index
        label_info.append(("sent", name))

    # Compact flat trie.  Signed chars hold labels -1..34.
    left = array("i", [-1])
    right = array("i", [-1])
    route_label = array("b", [-1])
    range_value = array("b", [-1])
    base_label = array("b", [_LABEL_PUNT])
    cost = array("I", [0])
    mask = array("Q", [0])

    def add_node():
        index = len(left)
        left.append(-1)
        right.append(-1)
        route_label.append(-1)
        range_value.append(-1)
        base_label.append(_LABEL_PUNT)
        cost.append(0)
        mask.append(0)
        return index

    def insert_prefix(network):
        node = 0
        prefix_int = int(network.network_address)
        for depth in range(network.prefixlen):
            bit = (prefix_int >> (31 - depth)) & 1
            children = right if bit else left
            child = children[node]
            if child == -1:
                child = add_node()
                children[node] = child
            node = child
        return node

    for network, egress in table:
        node = insert_prefix(network)
        if egress is None:
            route_label[node] = _LABEL_DROP
        else:
            try:
                route_label[node] = egress_label[egress]
            except KeyError as exc:
                raise ValueError(f"unknown configured egress: {egress!r}") from exc

    for network, allowed in range_rules:
        node = insert_prefix(network)
        range_value[node] = 1 if allowed else 0

    def solve(node, depth, inherited_route, inherited_range):
        own_route = route_label[node]
        effective_route = (
            own_route if own_route != -1 else inherited_route
        )
        own_range = range_value[node]
        effective_range = (
            own_range if own_range != -1 else inherited_range
        )
        target = (
            effective_route if effective_range else _LABEL_PUNT
        )
        base_label[node] = target

        if depth == 32:
            node_cost = 0
            node_mask = 1 << target
        else:
            child_data = []
            for child in (left[node], right[node]):
                if child == -1:
                    # A missing child has no more-specific input prefix, so
                    # its whole subtree is the uniform target label.
                    child_data.append((0, 1 << target))
                else:
                    child_data.append(
                        solve(
                            child,
                            depth + 1,
                            effective_route,
                            effective_range,
                        )
                    )

            left_cost, left_mask = child_data[0]
            right_cost, right_mask = child_data[1]
            common = left_mask & right_mask
            if common:
                node_cost = left_cost + right_cost
                node_mask = common
            else:
                node_cost = left_cost + right_cost + 1
                node_mask = left_mask | right_mask

        cost[node] = node_cost
        mask[node] = node_mask
        return node_cost, node_mask

    root_cost, root_mask = solve(
        0, 0, _LABEL_UNROUTED, 0
    )
    punt_bit = 1 << _LABEL_PUNT
    if root_mask & punt_bit:
        minimum_rows = root_cost
    else:
        minimum_rows = root_cost + 1

    if minimum_rows > max_rows:
        raise OffloadBudgetExceeded(
            budget=max_rows,
            minimum_rows=minimum_rows,
        )

    raw_rows = []

    def emit(node, depth, prefix_int, ancestor_label):
        node_mask = mask[node]
        ancestor_bit = 1 << ancestor_label
        if node_mask & ancestor_bit:
            chosen_label = ancestor_label
        else:
            # Installing the node with any label in its mask is optimal.
            chosen_bit = node_mask & -node_mask
            chosen_label = chosen_bit.bit_length() - 1
            action, egress = label_info[chosen_label]
            raw_rows.append((prefix_int, depth, action, egress))

        if depth == 32:
            return

        target = base_label[node]
        for bit_value, child in (
            (0, left[node]),
            (1, right[node]),
        ):
            child_prefix = prefix_int | (
                bit_value << (31 - depth)
            )
            if child == -1:
                if chosen_label != target:
                    action, egress = label_info[target]
                    raw_rows.append(
                        (child_prefix, depth + 1, action, egress)
                    )
            else:
                emit(
                    child,
                    depth + 1,
                    child_prefix,
                    chosen_label,
                )

    emit(0, 0, 0, _LABEL_PUNT)
    raw_rows.sort(key=lambda item: (item[0], item[1]))
    rows = tuple(
        (
            f"{IPv4Address(prefix_int)}/{depth}",
            action,
            egress,
        )
        for prefix_int, depth, action, egress in raw_rows
    )
    return rows, minimum_rows


class OffloadPlan:
    """Immutable offline image plus the matching captured software table."""

    __slots__ = ("_rows", "_software_routes")

    def __init__(self, rows, software_routes=()):
        self._rows = tuple(rows)
        # The active table is already an immutable tuple of frozen networks
        # and strings/None.  Copy the tuple reference contents defensively.
        self._software_routes = tuple(software_routes)

    @property
    def rows(self):
        return self._rows

    def resolve(self, destination, *, software_available=True):
        """Resolve via fast image first, then the captured software table."""
        address = _parse_ipv4_text(destination)
        action, egress = _lookup_image_parsed(self._rows, address)

        if action != "punt":
            return "fast", action, egress

        if not software_available:
            raise SoftwareUnavailable(
                "destination is punted to software, but software is unavailable"
            )

        outcome = Forwarder._decide(self._software_routes, address)
        return "software", outcome.action, outcome.egress


_IPV4_MASKS = tuple(
    (0xFFFFFFFF << (32 - length)) & 0xFFFFFFFF
    for length in range(33)
)


def _build_image_index(rows):
    """Build a length-keyed LPM index from valid exported image rows."""
    by_length = {}
    for row in rows:
        prefix_text, action, egress = row
        network = IPv4Network(prefix_text, strict=True)
        value = (action, egress if action == "sent" else None)
        by_length.setdefault(network.prefixlen, {})[
            int(network.network_address)
        ] = value
    return by_length


@lru_cache(maxsize=8)
def _cached_image_index(rows):
    # ``plan.rows`` is a hashable tuple of string/None tuples.  The bounded
    # cache keeps repeated standalone lookup_image calls inexpensive.
    return _build_image_index(rows)


def _get_image_index(rows):
    if isinstance(rows, tuple):
        try:
            return _cached_image_index(rows)
        except TypeError:
            # A tuple containing an unhashable row falls through to direct
            # indexing; valid exported rows are hashable.
            pass
    return _build_image_index(rows)


def _lookup_image_parsed(rows, address):
    """Lookup helper sharing one validated address object with resolve()."""
    address_int = int(address)
    by_length = _get_image_index(rows)

    for length in range(32, -1, -1):
        at_length = by_length.get(length)
        if at_length is None:
            continue
        network_address = address_int & _IPV4_MASKS[length]
        result = at_length.get(network_address)
        if result is not None:
            return result

    return "punt", None


class _Entry:
    __slots__ = (
        "index", "destination", "payload", "prefix", "egress",
        "active", "in_flight",
    )

    def __init__(self, index, destination, payload, outcome):
        self.index = index
        self.destination = destination
        self.payload = payload
        self.prefix = outcome.prefix
        self.egress = outcome.egress
        self.active = True
        self.in_flight = False


class _TicketState:
    __slots__ = ("ticket", "entries", "next_index", "in_flight")

    def __init__(self, ticket, entries):
        self.ticket = ticket
        self.entries = entries
        self.next_index = 0
        self.in_flight = False

    def active_count(self):
        return sum(1 for entry in self.entries if entry.active)


class Forwarder:
    """Route test datagrams using a replaceable longest-prefix table."""

    MAX_BATCH_SIZE = MAX_BATCH_SIZE
    MAX_ACTIVE_SAMPLES = MAX_ACTIVE_SAMPLES
    MAX_BATCH = MAX_BATCH_SIZE
    MAX_SAMPLES = MAX_ACTIVE_SAMPLES

    def __init__(self, egresses):
        self._egresses = dict(egresses)
        if not self._egresses or any(
            type(name) is not str or not name or not isinstance(address, tuple)
            or len(address) != 2 or type(address[0]) is not str
            or not IPv4Address(address[0]).is_loopback
            or type(address[1]) is not int or not 1 <= address[1] <= 65535
            for name, address in self._egresses.items()
        ):
            raise ValueError("egresses must be named IPv4 loopback destinations")
        self._routes = ()
        self._revision = 0
        self._lock = threading.RLock()
        self._tickets = {}
        self._active_count = 0
        self._next_ticket = 0
        self._forwarder_id = id(self)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._closed = False

    @property
    def revision(self):
        """Monotonic table revision; successful replacements increment it."""
        with self._lock:
            return self._revision

    @property
    def active_sample_count(self):
        """Number of accepted, unfinished managed samples (including sends)."""
        with self._lock:
            return self._active_count

    @staticmethod
    def _validate_destination_payload(destination, payload):
        # Keep the exact input rules and exception behavior of ``send``.
        if type(destination) is not str:
            raise ValueError("destination must be IPv4 text")
        address = IPv4Address(destination)
        if not isinstance(payload, bytes) or len(payload) > 4096:
            raise ValueError("expected at most 4096 bytes")
        return address

    @staticmethod
    def _decide(routes, address):
        for network, egress in routes:
            if address in network:
                if egress is None:
                    return Outcome("drop", str(network))
                return Outcome("sent", str(network), egress)
        return Outcome("unrouted")

    def _parse_candidate_routes(self, routes):
        table = {}
        for prefix, egress in routes:
            if type(prefix) is not str:
                raise ValueError("prefix must be text")
            network = IPv4Network(prefix, strict=True)
            if network in table or (egress is not None
                                    and (type(egress) is not str
                                         or egress not in self._egresses)):
                raise ValueError("duplicate prefix or unknown egress")
            table[network] = egress
        return tuple(sorted(table.items(), key=lambda entry: -entry[0].prefixlen))

    def replace(self, routes):
        """Set all (strict CIDR, egress name or None to drop) entries.

        The candidate is fully validated first.  Publication then takes the
        same lock used by reservation and claim/finish transitions, checks all
        unfinished managed promises, and atomically swaps the table.

        Operational choice for this implementation: an in-flight claimed send
        is still a promise.  If it is the only incompatible sample, ``replace``
        raises :class:`RouteProtected` immediately rather than waiting.  The
        caller may retry after ``dispatch_next`` returns.
        """
        candidate = self._parse_candidate_routes(routes)
        with self._lock:
            if self._closed:
                raise ForwarderError("forwarder socket is closed")
            conflicts = []
            for state in self._tickets.values():
                for entry in state.entries:
                    if not entry.active:
                        continue
                    new_outcome = self._decide(candidate, IPv4Address(entry.destination))
                    if (new_outcome.action != "sent"
                            or new_outcome.egress != entry.egress):
                        conflicts.append({
                            "ticket": state.ticket,
                            "index": entry.index,
                            "destination": entry.destination,
                            "promised_prefix": entry.prefix,
                            "promised_egress": entry.egress,
                            "in_flight": entry.in_flight,
                            "candidate_action": new_outcome.action,
                            "candidate_prefix": new_outcome.prefix,
                            "candidate_egress": new_outcome.egress,
                        })
            if conflicts:
                first = conflicts[0]
                raise RouteProtected(
                    "candidate table breaks promised egress "
                    f"{first['promised_egress']!r} for ticket sample "
                    f"{first['index']} (would become "
                    f"{first['candidate_action']}"
                    + (f":{first['candidate_egress']!r}"
                       if first["candidate_egress"] is not None else "")
                    + ")",
                    revision=self._revision,
                    conflicts=conflicts,
                )
            self._routes = candidate
            self._revision += 1

    def compile_offload(self, offload_ranges, max_rows):
        """Compile a fixed-size offline image from one consistent table snapshot.

        Only the immutable active-table reference is read under the forwarder
        lock.  All validation and compilation happen after releasing that lock;
        the returned plan retains the same table snapshot for software fallback.
        """
        if (
            isinstance(max_rows, bool)
            or not isinstance(max_rows, int)
            or not 0 <= max_rows <= MAX_OFFLOAD_ROWS
        ):
            raise ValueError(
                "max_rows must be an integer from 0 through "
                f"{MAX_OFFLOAD_ROWS}, and must not be bool"
            )

        with self._lock:
            # ``_routes`` is replaced atomically with a complete immutable
            # tuple.  Taking its reference here is the short synchronization
            # point with replace().
            table = self._routes

        if len(table) > MAX_OFFLOAD_ROUTES:
            raise ValueError(
                f"at most {MAX_OFFLOAD_ROUTES} active routes are supported "
                "for offload compilation"
            )
        if len(self._egresses) > MAX_OFFLOAD_EGRESSES:
            raise ValueError(
                f"at most {MAX_OFFLOAD_EGRESSES} configured egresses are "
                "supported for offload compilation"
            )

        range_rules = _parse_offload_ranges(offload_ranges)
        rows, _minimum_rows = _build_offload_image(
            table,
            tuple(self._egresses),
            range_rules,
            max_rows,
        )
        return OffloadPlan(rows, table)

    def send(self, destination, payload):
        """Return the actual action; a send error propagates without fallback."""
        address = self._validate_destination_payload(destination, payload)
        with self._lock:
            outcome = self._decide(self._routes, address)
            egress_address = (
                self._egresses[outcome.egress]
                if outcome.action == "sent" else None
            )
        if outcome.action != "sent":
            return outcome
        self._send_datagram(payload, egress_address)
        return outcome

    def reserve(self, samples):
        """Atomically validate and reserve an ordered sample batch.

        Returns an opaque :class:`BatchTicket`.  Raises a :class:`BatchRejected`
        subclass (often also a :class:`ValueError`) for an empty/invalid or
        unroutable/dropped batch, and :class:`CapacityError` when instance
        capacity is insufficient.
        """
        try:
            items = list(samples)
        except TypeError as exc:
            raise InvalidBatchEntry(
                None, "samples must be an iterable of (destination, payload) pairs"
            ) from exc

        if not items:
            raise EmptyBatch()
        if len(items) > self.MAX_BATCH_SIZE:
            raise BatchTooLarge(len(items), self.MAX_BATCH_SIZE)

        # Validate and route the complete batch before taking capacity.  The
        # state lock makes validation + registration one linearizable event
        # relative to replace().
        planned = []
        failures = []
        for index, item in enumerate(items):
            try:
                try:
                    destination, payload = item
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "entry must be a (destination, payload) pair"
                    ) from exc
                address = self._validate_destination_payload(destination, payload)
            except (ValueError, TypeError) as exc:
                failures.append({
                    "index": index,
                    "reason": "invalid",
                    "detail": str(exc),
                })
                continue
            # Decision is made under the same lock as registration below.
            planned.append((index, destination, payload, address))

        if failures:
            first = failures[0]
            raise InvalidBatchEntry(first["index"], first["detail"],
                                    failures=failures)

        with self._lock:
            if self._closed:
                raise ForwarderError("forwarder socket is closed")

            # Re-decide while holding the lock so no replacement can occur
            # between route selection and promise registration.  Check every
            # route before rejecting/registering.
            entries = []
            route_failure = None
            for index, destination, payload, address in planned:
                outcome = self._decide(self._routes, address)
                if outcome.action != "sent":
                    if route_failure is None:
                        route_failure = BatchRouteRejected(index, outcome)
                else:
                    entries.append(_Entry(index, destination, payload, outcome))
            if route_failure is not None:
                raise route_failure

            requested = len(entries)
            available = self.MAX_ACTIVE_SAMPLES - self._active_count
            if requested > available:
                raise CapacityError(
                    requested=requested,
                    available=available,
                    active=self._active_count,
                    limit=self.MAX_ACTIVE_SAMPLES,
                )

            self._next_ticket += 1
            ticket = BatchTicket(uuid.uuid4(), self._forwarder_id)
            state = _TicketState(ticket, entries)
            self._tickets[ticket] = state
            self._active_count += requested
            self._reservation_committed_locked(ticket, state)
            return ticket

    def _reservation_committed_locked(self, ticket, state):
        """Test/embedding seam after registration, while the lock is held."""

    def _get_state_locked(self, ticket):
        if not isinstance(ticket, BatchTicket):
            raise UnknownTicket("ticket was not issued by this Forwarder")
        if ticket._forwarder_id != self._forwarder_id:
            raise UnknownTicket("ticket belongs to another Forwarder instance")
        try:
            state = self._tickets[ticket]
        except (KeyError, TypeError):
            raise UnknownTicket("ticket is unknown or has already ended") from None
        return state

    def _send_datagram(self, payload, address):
        """Seam for deterministic send fixtures; production uses UDP sendto."""
        return self._socket.sendto(payload, address)

    def dispatch_next(self, ticket):
        """Send the next unsent sample for a ticket, in original order."""
        with self._lock:
            state = self._get_state_locked(ticket)
            if state.in_flight:
                raise BatchBusy("ticket already has a send that has not returned")
            if state.next_index >= len(state.entries):
                # Defensive: terminal tickets are normally removed immediately.
                self._tickets.pop(ticket, None)
                raise UnknownTicket("ticket has no remaining samples")
            entry = state.entries[state.next_index]
            if not entry.active:
                # Should not happen because successful entries advance the
                # cursor; keep this guard for invariant failures.
                self._tickets.pop(ticket, None)
                raise UnknownTicket("ticket entry has already ended")
            entry.in_flight = True
            state.in_flight = True
            payload = entry.payload
            address = self._egresses[entry.egress]
            promised = Outcome("sent", entry.prefix, entry.egress)

        # Never hold the state lock across the real send boundary.
        try:
            self._send_datagram(payload, address)
        except BaseException:
            with self._lock:
                current = self._tickets.get(ticket)
                if current is state:
                    self._terminate_locked(ticket, state)
            raise
        else:
            with self._lock:
                current = self._tickets.get(ticket)
                if current is state:
                    entry.in_flight = False
                    entry.active = False
                    self._active_count -= 1
                    state.in_flight = False
                    state.next_index = entry.index + 1
                    if state.next_index >= len(state.entries):
                        self._tickets.pop(ticket, None)
            return promised

    def _terminate_locked(self, ticket, state):
        """Release the claimed entry and all unsent entries after an error."""
        released = 0
        for entry in state.entries:
            if entry.active:
                entry.active = False
                entry.in_flight = False
                released += 1
        self._active_count -= released
        state.in_flight = False
        self._tickets.pop(ticket, None)
        return released

    def cancel(self, ticket):
        """Cancel unsent entries and return how many slots were released."""
        with self._lock:
            state = self._get_state_locked(ticket)
            if state.in_flight:
                raise BatchBusy("cannot cancel while a claimed send is outstanding")
            cancelled = 0
            for entry in state.entries[state.next_index:]:
                if entry.active:
                    entry.active = False
                    cancelled += 1
            self._active_count -= cancelled
            self._tickets.pop(ticket, None)
            return cancelled

    def close(self):
        """Abandon managed remainders and close the UDP socket."""
        with self._lock:
            self._closed = True
            self._tickets.clear()
            self._active_count = 0
        try:
            self._socket.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
