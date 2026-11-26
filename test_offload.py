"""Offline offload image compilation and fast/software resolution tests.

All addresses here are synthetic lookup inputs.  These tests never call
``send``/``dispatch_next`` through the new APIs and do not contact external
networks.  Scale cases are intended to be run under a 192 MiB address-space
limit, for example::

    (ulimit -v 196608; python3 -m unittest test_offload.py -v)
"""

from ipaddress import IPv4Address, IPv4Network
import socket
import unittest

try:  # Unix-only diagnostic used by the memory-scale acceptance run.
    import resource
except ImportError:  # pragma: no cover - portability fallback
    resource = None

from forwarding import (
    Forwarder,
    OffloadBudgetExceeded,
    SoftwareUnavailable,
    lookup_image,
)


def ip_text(value):
    return str(IPv4Address(value))


def dispersed_route_ip(index):
    """Map small indices to unique, dispersed synthetic 10/8 /32 inputs."""
    low24 = ((index + 1) * 2654435761) & 0xFFFFFF
    return f"10.{(low24 >> 16) & 255}.{(low24 >> 8) & 255}.{low24 & 255}"


def sparse_unrouted_grant_ip(index):
    """Unique synthetic 172.16/12 grant addresses not used by scale routes."""
    low16 = ((index + 1) * 40503) & 0xFFFF
    return f"172.16.{(low16 >> 8) & 255}.{low16 & 255}"


class OffloadTestBase(unittest.TestCase):
    def make_forwarder(self, egress_names):
        sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sink.bind(("127.0.0.1", 0))
        self.addCleanup(sink.close)

        egresses = {name: sink.getsockname() for name in egress_names}
        forwarder = Forwarder(egresses)
        self.addCleanup(forwarder.close)

        # Compilation and resolution must not reach the send seam.
        def fail_send(payload, address):
            raise AssertionError(f"offload path sent {payload!r} to {address}")

        forwarder._send_datagram = fail_send
        return forwarder

    def assert_canonical_rows(self, rows):
        self.assertIsInstance(rows, tuple)
        seen = set()
        for row in rows:
            self.assertIsInstance(row, tuple)
            self.assertEqual(len(row), 3)
            prefix, action, egress = row
            self.assertIsInstance(prefix, str)
            network = IPv4Network(prefix, strict=True)
            self.assertEqual(prefix, str(network), "row prefix is not canonical")
            self.assertNotIn(network, seen, "row prefixes must be unique")
            seen.add(network)
            self.assertIn(action, {"sent", "drop", "unrouted", "punt"})
            if action == "sent":
                self.assertIsInstance(egress, str)
            else:
                self.assertIsNone(egress)


class ExceptionHoleAndBudgetTest(OffloadTestBase):
    def test_budget_two_fails_three_rows_and_boundaries_are_correct(self):
        fw = self.make_forwarder(["east"])
        fw.replace([
            ("192.0.2.0/24", "east"),
            ("192.0.2.128/25", None),
        ])
        ticket = fw.reserve([("192.0.2.10", b"compile-must-not-touch")])
        ranges = [
            ("192.0.2.0/24", True),
            ("192.0.2.16/28", False),
        ]
        table_before = fw._routes
        revision_before = fw.revision

        with self.assertRaises(OffloadBudgetExceeded) as budget_ctx:
            fw.compile_offload(ranges, 2)
        rejected = budget_ctx.exception
        self.assertEqual(rejected.budget, 2)
        self.assertEqual(rejected.minimum_rows, 3)

        # A failed compile changes neither table, revision, nor tickets.
        self.assertIs(fw._routes, table_before)
        self.assertEqual(fw.revision, revision_before)
        self.assertEqual(fw.active_sample_count, 1)
        self.assertIn(ticket, fw._tickets)

        plan = fw.compile_offload(ranges, 3)
        self.assert_canonical_rows(plan.rows)
        self.assertEqual(len(plan.rows), 3)
        self.assertIs(fw._routes, table_before)
        self.assertEqual(fw.revision, revision_before)
        self.assertEqual(fw.active_sample_count, 1)

        with self.assertRaises(TypeError):
            plan.rows[0] = ("x", "punt", None)

        # Independent fixed policy, expressed with integer interval boundaries.
        net_24 = int(IPv4Address("192.0.2.0"))
        hole_start = int(IPv4Address("192.0.2.16"))
        drop_start = int(IPv4Address("192.0.2.128"))
        fixed_boundaries = {
            0,
            (1 << 32),
            net_24,
            net_24 + 256,
            hole_start,
            hole_start + 16,
            drop_start,
            drop_start + 128,
        }

        # Add every exported row's boundaries so each checked segment cannot
        # contain an image LPM transition that endpoint checks might miss.
        row_boundaries = set(fixed_boundaries)
        for prefix, _action, _egress in plan.rows:
            network = IPv4Network(prefix, strict=True)
            start = int(network.network_address)
            row_boundaries.add(start)
            row_boundaries.add(start + (1 << (32 - network.prefixlen)))
        boundaries = sorted(row_boundaries)

        def expected_fixed(address_int):
            if hole_start <= address_int < hole_start + 16:
                return ("punt", None)
            if net_24 <= address_int < net_24 + 256:
                if drop_start <= address_int < drop_start + 128:
                    return ("drop", None)
                return ("sent", "east")
            return ("punt", None)

        segments_checked = 0
        for low, high in zip(boundaries, boundaries[1:]):
            if low >= (1 << 32):
                continue
            high_inclusive = min(high - 1, (1 << 32) - 1)
            middle = low + (high_inclusive - low) // 2
            for address_int in (low, middle, high_inclusive):
                if address_int > high_inclusive:
                    continue
                text = ip_text(address_int)
                # Deliberately call the standalone function with only rows;
                # no Forwarder, table snapshot, or range object is passed.
                got = lookup_image(plan.rows, text)
                self.assertEqual(
                    got,
                    expected_fixed(address_int),
                    f"bad image result at {text} in segment [{low},{high})",
                )
                segments_checked += 1

        # Explicit acceptance addresses.
        self.assertEqual(
            lookup_image(plan.rows, "192.0.2.17"), ("punt", None)
        )
        self.assertEqual(
            lookup_image(plan.rows, "192.0.2.129"), ("drop", None)
        )
        self.assertEqual(
            lookup_image(plan.rows, "192.0.2.32"), ("sent", "east")
        )
        self.assertEqual(
            lookup_image(plan.rows, "192.0.1.255"), ("punt", None)
        )
        self.assertEqual(
            lookup_image(plan.rows, "192.0.3.0"), ("punt", None)
        )

        print(
            "\n[offload acceptance 1] "
            f"budget_2_rejection={type(rejected).__name__}"
            f"(minimum_rows={rejected.minimum_rows}); "
            f"budget_3_actual_rows={len(plan.rows)}; rows={list(plan.rows)}; "
            f"boundary_samples_checked={segments_checked}",
            flush=True,
        )


class ResolvePathsAndSnapshotTest(OffloadTestBase):
    def test_same_address_changes_only_range_authorization(self):
        fw = self.make_forwarder(["east"])
        fw.replace([
            ("192.0.2.0/24", "east"),
            ("192.0.2.128/25", None),
        ])
        hole_ranges = [
            ("192.0.2.0/24", True),
            ("192.0.2.16/28", False),
        ]
        hole_plan = fw.compile_offload(hole_ranges, 3)

        online = hole_plan.resolve(
            "192.0.2.17", software_available=True
        )
        self.assertEqual(online, ("software", "sent", "east"))

        with self.assertRaises(SoftwareUnavailable) as unavailable_ctx:
            hole_plan.resolve("192.0.2.17", software_available=False)
        unavailable_type = type(unavailable_ctx.exception).__name__

        # Remove only the exclusion and additionally authorize a route-less
        # network.  Budget 3 remains sufficient.
        broad_ranges = [
            ("192.0.2.0/24", True),
            ("198.51.100.0/24", True),
        ]
        broad_plan = fw.compile_offload(broad_ranges, 3)
        self.assert_canonical_rows(broad_plan.rows)
        self.assertLessEqual(len(broad_plan.rows), 3)

        fast_east = broad_plan.resolve(
            "192.0.2.17", software_available=False
        )
        self.assertEqual(fast_east, ("fast", "sent", "east"))

        fast_unrouted = broad_plan.resolve(
            "198.51.100.9", software_available=False
        )
        self.assertEqual(fast_unrouted, ("fast", "unrouted", None))

        neighbor_results = {}
        for neighbor in ("198.51.99.255", "198.51.101.0"):
            self.assertEqual(
                lookup_image(broad_plan.rows, neighbor), ("punt", None)
            )
            with self.assertRaises(SoftwareUnavailable):
                broad_plan.resolve(neighbor, software_available=False)
            neighbor_results[neighbor] = broad_plan.resolve(
                neighbor, software_available=True
            )
            self.assertEqual(
                neighbor_results[neighbor],
                ("software", "unrouted", None),
            )

        print(
            "\n[offload acceptance 2] "
            f"hole_.17_online_path={online}; "
            f"hole_.17_offline_rejection={unavailable_type}; "
            f"reauthorized_.17_path={fast_east}; "
            f"route_less_grant_path={fast_unrouted}; "
            f"actual_broad_rows={len(broad_plan.rows)}; "
            f"unauthorized_neighbor_software_paths={neighbor_results}",
            flush=True,
        )

    def test_old_plan_uses_captured_table_after_replace(self):
        fw = self.make_forwarder(["east", "west"])
        fw.replace([
            ("192.0.2.0/24", "east"),
            ("192.0.2.128/25", None),
        ])
        ranges = [
            ("192.0.2.0/24", True),
            ("192.0.2.16/28", False),
        ]
        old_plan = fw.compile_offload(ranges, 3)
        self.assertEqual(
            old_plan.resolve("192.0.2.17"),
            ("software", "sent", "east"),
        )
        old_rows = old_plan.rows

        fw.replace([
            ("192.0.2.0/24", "west"),
            ("192.0.2.128/25", None),
            ("192.0.2.17/32", None),
        ])
        self.assertEqual(fw.revision, 2)

        # The old image and its software fallback are both snapshots.
        self.assertEqual(old_plan.rows, old_rows)
        self.assertEqual(
            old_plan.resolve("192.0.2.17"),
            ("software", "sent", "east"),
        )

        new_plan = fw.compile_offload(ranges, 3)
        self.assertEqual(
            new_plan.resolve("192.0.2.17", software_available=True),
            ("software", "drop", None),
        )
        with self.assertRaises(SoftwareUnavailable):
            new_plan.resolve("192.0.2.17", software_available=False)

        print(
            "\n[offload snapshot] "
            "after_replace_revision=2; "
            "old_plan_.17=software/sent/east; "
            "new_compile_.17=software/drop; "
            f"new_plan_rows={list(new_plan.rows)}",
            flush=True,
        )


class EmptySparseAndValidationTest(OffloadTestBase):
    def test_empty_allow_set_zero_budget_and_single_unrouted_point(self):
        fw = self.make_forwarder(["east"])
        fw.replace([
            ("192.0.2.0/24", "east"),
            ("192.0.2.128/25", None),
        ])

        empty_plan = fw.compile_offload([], 0)
        self.assertEqual(empty_plan.rows, ())
        self.assertEqual(
            lookup_image(empty_plan.rows, "198.51.100.1"),
            ("punt", None),
        )
        with self.assertRaises(SoftwareUnavailable):
            empty_plan.resolve(
                "198.51.100.1", software_available=False
            )
        self.assertEqual(
            empty_plan.resolve("198.51.100.1", software_available=True),
            ("software", "unrouted", None),
        )

        point_range = [("198.51.100.7/32", True)]
        with self.assertRaises(OffloadBudgetExceeded) as ctx:
            fw.compile_offload(point_range, 0)
        self.assertEqual(ctx.exception.minimum_rows, 1)

        point_plan = fw.compile_offload(point_range, 1)
        self.assert_canonical_rows(point_plan.rows)
        self.assertEqual(len(point_plan.rows), 1)
        self.assertEqual(
            point_plan.resolve(
                "198.51.100.7", software_available=False
            ),
            ("fast", "unrouted", None),
        )
        for neighbor in ("198.51.100.6", "198.51.100.8"):
            self.assertEqual(
                lookup_image(point_plan.rows, neighbor), ("punt", None)
            )
            with self.assertRaises(SoftwareUnavailable):
                point_plan.resolve(neighbor, software_available=False)

        print(
            "\n[offload acceptance 3] "
            "empty_ranges_budget_0=success(rows=0); "
            "single_route_less_/32_budget_0_rejection="
            f"{type(ctx.exception).__name__}"
            f"(minimum_rows={ctx.exception.minimum_rows}); "
            f"budget_1_rows={list(point_plan.rows)}",
            flush=True,
        )

    def test_invalid_inputs_and_ipv4_text_are_value_errors(self):
        fw = self.make_forwarder(["east"])
        fw.replace([("0.0.0.0/0", "east")])
        revision = fw.revision

        bad_budgets = (-1, 8193, True, False, "3", 1.5, None)
        for budget in bad_budgets:
            with self.subTest(budget=budget):
                with self.assertRaises(ValueError):
                    fw.compile_offload([], budget)
                self.assertEqual(fw.revision, revision)

        bad_ranges = [
            [("192.0.2.0/24", True), ("192.0.2.0/24", False)],
            [("192.0.2.1/24", True)],
            [("2001:db8::/32", True)],
            [("not-an-ip", True)],
            [("192.0.2.0/24", 1)],
            [(12345, True)],
            [("192.0.2.0/24", "yes")],
            [None],
            [("192.0.2.0/24",)],
        ]
        for ranges in bad_ranges:
            with self.subTest(ranges=ranges):
                with self.assertRaises(ValueError):
                    fw.compile_offload(ranges, 8192)
                self.assertEqual(fw.revision, revision)

        plan = fw.compile_offload([], 0)
        bad_addresses = (
            "not-ipv4",
            "203.0.113.1/24",
            "2001:db8::1",
            None,
            12345,
            b"192.0.2.1",
        )
        for address in bad_addresses:
            with self.subTest(address=address):
                with self.assertRaises(ValueError):
                    lookup_image(plan.rows, address)
                with self.assertRaises(ValueError):
                    plan.resolve(address)

    def test_compile_scale_limits_are_value_errors(self):
        # Boundary budgets are valid; bool is not an integer budget.
        fw_zero = self.make_forwarder(["east"])
        self.assertEqual(fw_zero.compile_offload([], 0).rows, ())
        fw_boundary = self.make_forwarder(["east"])
        self.assertEqual(
            fw_boundary.compile_offload([], 8192).rows, ()
        )

        fw = self.make_forwarder(["east"])
        too_many_ranges = [
            (f"10.{(i >> 8) & 255}.{i & 255}.0/24", True)
            for i in range(4097)
        ]
        with self.assertRaises(ValueError):
            fw.compile_offload(too_many_ranges, 8192)

        too_many_egresses = self.make_forwarder(
            [f"egress_{i}" for i in range(33)]
        )
        with self.assertRaises(ValueError):
            too_many_egresses.compile_offload([], 0)

        # Replacement itself remains unrestricted by the offload limits.
        too_many_routes = [
            (f"10.{(i >> 8) & 255}.{i & 255}.{j}/32", "east")
            for i in range(17)
            for j in range(256)
        ]
        fw.replace(too_many_routes)
        self.assertGreater(len(fw._routes), 4096)
        with self.assertRaises(ValueError):
            fw.compile_offload([], 0)


class ScaleSparseSyntheticTest(OffloadTestBase):
    def test_4096_routes_4096_ranges_32_egresses_budget_4096(self):
        egress_names = [f"e{i}" for i in range(32)]
        fw = self.make_forwarder(egress_names)

        routes = [
            (f"{dispersed_route_ip(i)}/32", f"e{i % 32}")
            for i in range(4096)
        ]
        fw.replace(routes)

        ranges = [
            (f"{dispersed_route_ip(i)}/32", True)
            for i in range(2048)
        ]
        ranges.extend(
            (f"{sparse_unrouted_grant_ip(j)}/32", True)
            for j in range(2048)
        )

        plan = fw.compile_offload(ranges, 4096)
        self.assert_canonical_rows(plan.rows)
        self.assertLessEqual(len(plan.rows), 4096)

        authorized_routed_index = 123
        authorized_routed_ip = dispersed_route_ip(authorized_routed_index)
        expected_egress = f"e{authorized_routed_index % 32}"
        self.assertEqual(
            lookup_image(plan.rows, authorized_routed_ip),
            ("sent", expected_egress),
        )
        self.assertEqual(
            plan.resolve(
                authorized_routed_ip, software_available=False
            ),
            ("fast", "sent", expected_egress),
        )

        authorized_unrouted_ip = sparse_unrouted_grant_ip(77)
        self.assertEqual(
            lookup_image(plan.rows, authorized_unrouted_ip),
            ("unrouted", None),
        )
        self.assertEqual(
            plan.resolve(
                authorized_unrouted_ip, software_available=False
            ),
            ("fast", "unrouted", None),
        )

        unauthorized_routed_index = 3000
        unauthorized_routed_ip = dispersed_route_ip(
            unauthorized_routed_index
        )
        self.assertEqual(
            lookup_image(plan.rows, unauthorized_routed_ip),
            ("punt", None),
        )
        with self.assertRaises(SoftwareUnavailable):
            plan.resolve(
                unauthorized_routed_ip, software_available=False
            )
        self.assertEqual(
            plan.resolve(unauthorized_routed_ip),
            (
                "software",
                "sent",
                f"e{unauthorized_routed_index % 32}",
            ),
        )

        if resource is not None:
            max_rss_kib = resource.getrusage(
                resource.RUSAGE_SELF
            ).ru_maxrss
        else:  # pragma: no cover
            max_rss_kib = "unavailable"
        print(
            "\n[offload scale] "
            f"routes=4096 ranges=4096 egresses=32 budget=4096; "
            f"actual_rows={len(plan.rows)}; "
            f"max_rss_kib={max_rss_kib}; "
            "sampled=authorized+routed, authorized+unrouted, "
            "unauthorized+routed",
            flush=True,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
