from contextlib import contextmanager
import socket

from forwarding import Forwarder


@contextmanager
def receiver():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.settimeout(2)
        yield sock


def run():
    with receiver() as west, receiver() as east, Forwarder(
        {"west": west.getsockname(), "east": east.getsockname()}
    ) as router:
        router.replace([("0.0.0.0/0", "west"), ("198.51.100.0/24", "east"),
                        ("198.51.100.128/25", None)])
        assert router.send("198.51.100.12", b"sample").egress == "east"
        chosen = east.recv(64)
        dropped = router.send("198.51.100.150", b"drop").action
        assert dropped == "drop"
        router.replace([("0.0.0.0/0", "west")])
        assert router.send("198.51.100.12", b"sample").egress == "west"
        switched = west.recv(64)
        return chosen, dropped, switched


if __name__ == "__main__":
    chosen, dropped, switched = run()
    print("specific -> east:", chosen)
    print("more specific ->", dropped)
    print("after replacement -> west:", switched)
