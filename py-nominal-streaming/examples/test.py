import logging
import pathlib
import time

from nominal.core import NominalClient
from nominal_streaming import NominalDatasetStream, NominalStreamOpts

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    num_rows = 100_000
    num_batches = 1000
    client = NominalClient.from_profile("production")
    stream = (
        NominalDatasetStream(
            auth_header=client._clients.auth_header.split()[-1],
            opts=NominalStreamOpts.default()
            .with_num_upload_workers(16)
            .with_max_buffered_requests(4)
            .with_num_runtime_workers(20)
            .with_max_points_per_record(100_000),
        )
        .enable_logging("info")
        .with_core_consumer(client.create_dataset("drake test").rid)
        # .to_file(pathlib.Path("test.avro"))
    )
    start_time = time.time()
    offset = 1.0 / 1600
    curr_offset = 0.0
    values = [1.3 + idx for idx in range(num_rows)]
    with stream:
        for batch_idx in range(num_batches):
            batch_build_start = time.time()
            timestamps = [int((start_time + curr_offset + offset * idx) * 1e9) for idx in range(num_rows)]
            curr_offset += num_rows * offset
            batch_build_end = time.time()
            stream.enqueue_batch("drake_test_channel4", timestamps, values)
            batch_enqueue_end = time.time()

            print(
                "Batch took",
                batch_enqueue_end - batch_build_start,
                "seconds; build took",
                batch_build_end - batch_build_start,
                "seconds; enqueue took",
                batch_enqueue_end - batch_build_end,
                "seconds",
            )
        enqueue_end = time.time()
    end_time = time.time()

    print(enqueue_end - start_time, end_time - enqueue_end, end_time - start_time)
