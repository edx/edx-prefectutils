import gc
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Iterable, List, Sequence

import backoff
import requests
import snowflake.connector
from prefect import task

from .snowflake import (LazySnowflakeResultList, SFCredentials,
                        create_snowflake_connection)

AMPLITUDE_REQUEST_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*'
}
PROCESSING_BUCKET_SIZE = 1000000
MAX_CONCURRENT_AMPLITUDE_CONNECTIONS = 64


def _batch_generator(sequence: Sequence, batch_size: int = 1000) -> Generator[Sequence, None, None]:
    """
    This is just a simple batching algorithm, implemented as a generator.

    Args:
      sequence (Sequence): all the individual elements to group into batches.  Must support len() and random indexing.
      batch_size (int): the number of elements in each batch.

    Returns:
      A generator of batches, where each batch is effectively an iterable
      slice of the input.
    """
    total_length = len(sequence)
    for batch_start in range(0, total_length, batch_size):
        batch_end = min(batch_start + batch_size, total_length)
        yield sequence[batch_start:batch_end]


def _fetch_and_consolidate_and_batch(
    connection: snowflake.connector.SnowflakeConnection, query: str, batch_size: int = 1000, fetch_size: int = 10000
) -> Iterable[Iterable[Sequence[dict]]]:
    """
    Given a snowflake query returning an entire processing segment of amplitude events, fetch and allocate events to
    workers such that the loading speed into amplitude is optimized.

    Argument `query` must:

    - Make consecutive the events with the same user_id/device_id segment, i.e. coalesce(user_id, device_id).
    - Sort user_id/device_id segments in descending order by rowcount of segments.
    - Sort events by increasing timestamp within each user_id/device_id segment.

    This function will consolidate segments smaller than batch_size events such that the resulting consolidated segments
    don't exceed batch_size events.  Then, it will batch any segments still larger than batch_size. The final product
    should look like an iterable of iterable of iterable of events. I.e.:

    - First level loops over user/device segment (each of these will be allocated to a different worker thread).
    - Second level loops over batch_size batches of events (this is a requirement imposed by the Amplitude API).
    - Third level loops over individual events.

    Memory Efficiency: In practice, this function won't fetch all events for the current processing segment at once;
    instead it will fetch small amounts until there are enough events to go around for all the worker threads.  The
    best-case-scenario memory consumption would be about N_worker_threads * batch_size * event_size, which is tiny.
    However, the worse-case-scenario is that somehow a large proportion of the events in this processing segment
    corresponds to a single user, in which case the memory consumption would equal that of loading the entire processing
    segment into memory (multiple gigabytes).  We could address memory consumption by adding even more complexity (and
    possibly bugs) to the code, but this is an exceedingly rare scenario that isn't worth optimizing for.

    Args:
        connection (snowflake.connector.SnowflakeConnection): A snowflake connection object.
        query (str): A snowflake query.
        batch_size (int): Maximum number of events to include in a single Amplitude API call.  This cannot be greater
            than 1000 since that is currently the maximum imposed by Amplitude.
        fetch_size (int): The "small" number of events to fetch from snowflake each time we need to ask for more.

    Returns:
        iterable of iterable of iterable of events.
    """
    accumulated_events = LazySnowflakeResultList(connection, query, fetch_size=fetch_size)
    end_idx_for_worker = None
    while True:
        if accumulated_events.logical_length() <= batch_size:
            # All we have left is one or more segments that completely fit into one batch, so send it all!
            end_idx_for_worker = len(accumulated_events)
        elif accumulated_events[0].user_device == accumulated_events[batch_size].user_device:
            # We have a segment which is larger than a single bucket.  We should find the last event for that
            # segment and send the entire segment to a worker.
            target_user_device = accumulated_events[0].user_device
            idx = batch_size
            while True:
                # Is there a more efficient way than indexing one event at a time?  Yes, but simple iteration is not
                # our performance bottleneck, and anything more complicated can introduce bugs.
                idx += 1
                try:
                    if accumulated_events[idx].user_device != target_user_device:
                        end_idx_for_worker = idx
                        break
                except IndexError:
                    end_idx_for_worker = idx
                    break
        else:
            # One or more segments completely fit into one batch, so lets consolidate them and send them.  Exclude
            # the last segment so that we send no more than one-batch-worth of events by finding the first event for
            # the last segment and using that as a cutoff:
            cut_user_device = accumulated_events[batch_size].user_device
            idx = batch_size
            while True:
                idx -= 1
                if accumulated_events[idx].user_device != cut_user_device:
                    end_idx_for_worker = idx + 1
                    break

        # Now we have `end_idx_for_worker` set to the point where we want to slice off events to send to a worker
        # thread.  Also, it is important that we delete that slice from the accumulated_events so that the python
        # garbage collector will free all that memory as soon as the corresponding worker thread returns.
        events_for_next_worker = accumulated_events[:end_idx_for_worker]
        del accumulated_events[:end_idx_for_worker]

        # Finally, batch the events such that each batch is small enough to fit inside a single Amplitude API call.
        yield _batch_generator(events_for_next_worker, batch_size)


def _get_old_high_watermark(connection: snowflake.connector.SnowflakeConnection) -> int:
    """
    Get the amplitude event ID of the event immediately following the very last event sent to Amplitude's API.

    Raises:
        snowflake.connector.ProgrammingError: if the loader log table is inaccessible.
    """
    cursor = connection.cursor()
    query = """
        select max(amplitude_event_id)
          from prod.amplitude_events.amplitude_events_loader_log
        """
    cursor.execute(query)
    old_high_watermark = cursor.fetchone()[0]
    if not old_high_watermark:
        old_high_watermark = 0
    return old_high_watermark


def _get_new_high_watermark(connection: snowflake.connector.SnowflakeConnection) -> int:
    """
    Get the last amplitude_event_id (inclusive) which should be processed as part of this flow.

    Args:
        TODO

    Raises:
        snowflake.connector.ProgrammingError: if the amplitude_events table is inaccessible.
    """
    cursor = connection.cursor()
    query = """
        select max(amplitude_event_id)
          from prod.amplitude_events.amplitude_events
        """
    cursor.execute(query)
    return cursor.fetchone()[0]


def _set_high_watermark(connection: snowflake.connector.SnowflakeConnection, new_high_watermark: int):
    """
    Add a new watermark record to the loader log.

    Args:
        TODO

    Raises:
        snowflake.connector.ProgrammingError: if the loader log table is inaccessible.
    """
    cursor = connection.cursor()
    query = f"""
        insert into prod.amplitude_events.amplitude_events_loader_log  (timestamp, amplitude_event_id)
            values (current_timestamp()::timestamp_tz, {new_high_watermark})
        """
    cursor.execute(query)
    connection.commit()


@backoff.on_exception(
    backoff.expo,
    (
        requests.exceptions.HTTPError,  # i.e. the status is 4xx or 5xx.
        requests.exceptions.ConnectionError,  # This includes connection timeout.
        requests.exceptions.Timeout,  # This includes read timeout.
        # In other data pipelines, retrying on read timeout could be dangerous because the events from the
        # failed request my have been successfully ingested, so retrying can result in duplicate events.
        # However, we do not mind retrying on read timeout with Amplitude loading because Amplitude will de-dupe
        # the events anyway.
    ),
)
def _send_batch_to_amplitude(
    amplitude_events_batch: Sequence[dict], finite_senders_lock: threading.Semaphore, amplitude_api_key: str
):
    """
    Actually make a Batch API request to send a batch of events.

    This is meant to be executed by a worker thread only.  Since we only want to allow a finite number of worker
    threads to be running API calls simultaneously, the API call is surrounded by logic to acquire/release a
    semaphore lock.  It's important that this semaphore logic exists *inside* the retry loop because we don't
    want to block any other workers while this one is in the middle of backing off.

    Args:
        amplitude_events_batch (iterable of events): 1000 or fewer events formatted for consumption by
            Amplitude.

    Raises:
        TODO
    """
    with finite_senders_lock:
        r = requests.post(
            "https://api2.amplitude.com/2/httpapi",
            json={
                "api_key": amplitude_api_key,
                "events": amplitude_events_batch,
                "options": {
                    "min_id_length": 1,  # valid edX user IDs start in the single-digis.
                },
            },
            headers=AMPLITUDE_REQUEST_HEADERS,
        )
        r.raise_for_status()


def _send_user_device_segment(
    user_device_segment: Iterable[Sequence[dict]], finite_senders_lock: threading.Semaphore, amplitude_api_key: str
):
    """
    Load all the events for a given device within the current processing segment into Amplitude.

    Args:
        segment(iterable of iterable of events): All events for single user or device, batched by 1000.

    Raises:
        TODO
    """
    for api_batch in user_device_segment:
        _send_batch_to_amplitude(api_batch, finite_senders_lock, amplitude_api_key)


@task
def get_processing_segments(sf_credentials: SFCredentials, sf_role: str) -> List[tuple[int, int]]:
    """
    """
    sf_connection = create_snowflake_connection(sf_credentials, sf_role)

    # Get the old (i.e. current) high watermark.  This is the ID of the last event that was loaded.
    old_high_watermark = _get_old_high_watermark(sf_connection)

    # Get the new high watermark, i.e. the current maximum amplitude_event_id.  Thi sis the last event that we plan to
    # load.
    new_high_watermark = _get_new_high_watermark(sf_connection)

    # Get a list of processing segments which cover all events between the old and new high watermark. The result is a
    # list of start/end tuples describing amplitude_event_id ranges, where each range represents a single processing
    # segment with the configured batch size.  The start ID of each segment is inclusive, whereas the end ID is
    # exclusive; this is pythonic.
    #
    # e.g. if old=999999, new=3599999, bucketsize=1000000, then:
    # processing_segments = [
    #     (1000000, 2000000),
    #     (2000000, 3000000),
    #     (3000000, 3600000),
    # ]
    #
    # In the above example, the last batch is smaller than 1m events because there are no more events to include.
    return [
        (processing_segment_start, min(processing_segment_start + PROCESSING_BUCKET_SIZE, new_high_watermark + 1))
        for processing_segment_start in range(old_high_watermark + 1, new_high_watermark + 1, PROCESSING_BUCKET_SIZE)
    ]


@task
def load_events_into_amplitude(
    sf_credentials: SFCredentials, sf_role: str, amplitude_api_key: str, processing_segments: List[tuple[int, int]]
):
    """
    """
    # Iterate over processing segments in chronological order, handling each one
    # one at a time, and completely freeing the memory of the previous one before
    # fetching the next.
    for processing_segment in processing_segments:
        sf_connection = create_snowflake_connection(sf_credentials, sf_role)

        # Prep a cursor to fetch the events for the current processing segment.
        #
        # For events with amplitude_event_id within the current processing_segment, further segment the events by the
        # following dimension:
        #
        #   coalesce(user_id, device_id)
        #
        # Then, sort events by segment size and sort by timestamp within each segment.
        processing_segment_start, processing_segment_end = processing_segment
        segmented_by_user_device_query = f"""
              select coalesce(user_id, device_id) as user_device,
                     count(*) over (partition by user_device) as user_device_event_count,
                     *
                from events_to_load
               where amplitude_event_id >= {processing_segment_start}
                 and amplitude_event_id < {processing_segment_end}
            order by user_device_event_count asc,
                     user_device asc,
                     timestamp asc
            """

        # Make a generator which can fetch events and slice them for worker threads.
        segmented_by_user_device_and_batched = _fetch_and_consolidate_and_batch(
            sf_connection, segmented_by_user_device_query
        )

        # Only allow a fixed number of simultaneous network calls.
        finite_senders_lock = threading.Semaphore(MAX_CONCURRENT_AMPLITUDE_CONNECTIONS)

        # Launch a thread pool with twice as many threads as the the connection limit so that we have a few extra
        # threads to pick up the slack whenever a thread gets stuck backing off due to EPDS rate limiting.  In theory,
        # any time we have fewer than the maximum number of blocking network calls, we are running slower than capacity.
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_AMPLITUDE_CONNECTIONS * 2) as executor:
            executor.map(
                _send_user_device_segment,
                segmented_by_user_device_and_batched,
                itertools.repeat(finite_senders_lock),
                itertools.repeat(amplitude_api_key),
            )

        # Update the high watermark.  processing_segment_end represents the next index after the last index loaded, so
        # subtract 1 to get a valid watermark.
        _set_high_watermark(sf_connection, processing_segment_end - 1)

        # This shouldn't be necessary, but make sure that we aren't deferring any garbage collection since the next
        # iteration may start to allocate a lot of memory.
        del segmented_by_user_device_and_batched
        gc.collect()
