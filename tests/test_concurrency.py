"""并发接入：多线程下去重计数、水位与缓冲必须正确不错乱。"""
import threading

from gateway.buffer import OverflowPolicy
from gateway.model import RejectReason


def test_concurrent_ingest_unique_seqs(make_gateway, device):
    gw = make_gateway(window=1000, capacity=10000)
    n = 200
    errors: list[Exception] = []

    def worker(start: int):
        try:
            for seq in range(start, n + 1, 2):
                enc = device.emit_register if seq % 3 else device.emit_json
                gw.ingest(enc(seq, "m", float(seq), float(seq)))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors
    seqs = sorted(r.seq for r in gw.downstream.received)
    assert seqs == list(range(1, n + 1))           # 无丢失无重复
    assert gw.downstream.received_count == n
    assert gw.stats()["watermarks"]["dev-1"] == n
    assert gw.downstream.duplicate_delivery == 0


def test_concurrent_replays_never_double_count(make_gateway, device):
    gw = make_gateway(window=1000, capacity=10000)
    n = 100
    frames = [
        (device.emit_register if s % 2 else device.emit_json)(s, "m", s, s)
        for s in range(1, n + 1)
    ]
    # 先正常送入
    for f in frames:
        gw.ingest(f)
    assert gw.downstream.received_count == n

    barrier = threading.Barrier(4)

    def replay():
        barrier.wait()
        for f in frames:
            gw.ingest(f)

    threads = [threading.Thread(target=replay) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = gw.stats()
    assert stats["downstream_received"] == n
    assert stats["accepted_readings"] == n
    # 所有重放必须被判为重复或回退，数量恰好为 4*n
    assert stats["rejected_duplicate"] + stats["rejected_rollback"] == 4 * n
    assert gw.downstream.duplicate_delivery == 0


def test_concurrent_outage_then_restore(make_gateway, device):
    gw = make_gateway(window=1000, capacity=10000,
                      policy=OverflowPolicy.DROP_NEWEST)
    n = 300
    gw.downstream.set_available(False)

    def worker(start: int):
        for seq in range(start, n + 1, 2):
            gw.ingest(device.emit_json(seq, "m", seq, seq))

    threads = [threading.Thread(target=worker, args=(1,)),
               threading.Thread(target=worker, args=(2,))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert gw.stats()["accepted_readings"] == n
    gw.downstream.set_available(True)
    sent = gw.on_downstream_restored()
    assert sent == n
    assert sorted(r.seq for r in gw.downstream.received) == list(range(1, n + 1))
