import time

from nominal_streaming import NominalStreamOpts, NominalDatasetStream
import nominal

if __name__ == "__main__":
    client = nominal.NominalClient.from_profile("production")
    ds = client.create_dataset("bound stream test")
    opts = NominalStreamOpts.default().with_request_dispatcher_tasks(56)
    stream = NominalDatasetStream(num_runtime_threads=64)
    stream.with_core_consumer(client._clients.auth_header.split()[-1], ds.rid)
    with stream.open() as ws:
        start = time.time()
        for batch_idx in range(1000):
            # for idx in range(100000000):
            #     ws.enqueue("test channel 2", int(idx * 1e9), 12.3, {"apple": "banana"})
            data = [idx + 1.2 for idx in range(100_000)]
            ts = [int(idx + 1e9 * batch_idx) for idx in range(100_000)]
            ws.enqueue_batch("test channel batch", ts, data, {"apple": "banana"})
            
        enqueue_end = time.time()
    flush_end = time.time()
    end = time.time()
    print("Took", end - start, "seconds (", flush_end - enqueue_end, ")"))
