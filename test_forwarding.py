from unittest import TestCase

from demo import receiver, run
from forwarding import Forwarder, Outcome


class ForwarderTest(TestCase):
    def test_same_datagram_changes_real_egress(self):
        self.assertEqual(run(), (b"sample", "drop", b"sample"))

    def test_policy_and_unrouted_never_send(self):
        with receiver() as sock, Forwarder({"main": sock.getsockname()}) as router:
            router.replace([("192.0.2.0/24", None), ("192.0.2.3/32", "main")])
            self.assertEqual(router.send("192.0.2.2", b"blocked"), Outcome("drop", "192.0.2.0/24"))
            self.assertEqual(router.send("198.51.100.1", b"unrouted"), Outcome("unrouted"))
            router.send("192.0.2.3", b"allowed")
            self.assertEqual(sock.recv(32), b"allowed")
            sock.setblocking(False)
            with self.assertRaises(BlockingIOError):
                sock.recv(32)

    def test_invalid_replacement_keeps_prior_table(self):
        with receiver() as sock, Forwarder({"main": sock.getsockname()}) as router:
            router.replace([("0.0.0.0/0", "main")])
            for bad in ([("192.0.2.1/24", "main")],
                        [("192.0.2.0/24", "missing")],
                        [("192.0.2.0/24", None), ("192.0.2.0/24", None)]):
                with self.assertRaises(ValueError):
                    router.replace(iter(bad))
            self.assertEqual(router.send("192.0.2.1", b"prior"), Outcome("sent", "0.0.0.0/0", "main"))
            self.assertEqual(sock.recv(32), b"prior")
            with self.assertRaises(ValueError):
                router.send("::1", b"unsupported")

    def test_egress_is_local_only(self):
        with self.assertRaises(ValueError):
            Forwarder({"remote": ("192.0.2.1", 8000)})
