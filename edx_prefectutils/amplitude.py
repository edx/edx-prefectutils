import backoff
#from prefect import task
#from prefect.engine import signals
from prefect.utilities.logging import get_logger

AMPLITUDE_REQUEST_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*'
}

def batch_generator(iterable, batch_size=1000):
    """
    This is just a simple batching algorithm, implemented as a generator.

    Args:
      iterable (iterable): all the individual elements to group into batches.
      batch_size (int): the number of elements in each batch.

    Returns:
      A generator of batches, where each batch is effectively an iterable a
      slice of the input.
    """
    total_length = len(iterable)
    for batch_start in range(0, total_length, batch_size):
        batch_end = min(batch_start + batch_size, total_length)
        yield iterable[batch_start:batch_end]


class LazyResultFetcher(list):
    """
    This creates a lazy list-like object which represents a complete snowflake result set by deferring fetching batches
    of results until they are indexed.

    Usage: Invoke cursor.execute(query), create a new LazyResultFetcher providing the cursor to the constructor, and treat
    the resulting LazyResultFetcher object as a list containing every row in the complete result set, with the caveat that
    len() will return the count of the actual contents.  Keep indexing higher and higher until you get an IndexError.

    Slicing this object always "downgrades" the result to a standard list, so we can safely pass slices around to other
    worker threads without worrying that indexing those slices will inadvertently cause snowflake batch fetches.
    """

    def __init__(self, cursor, fetch_size=10000):
        self._cursor = cursor
        self._fetch_size = fetch_size
        self._results_exhausted = False
        self._total_rowcount = cursor.rowcount
        self._fetched_rowcount = 0
        super(LazyResultFetcher, self).__init__()

    def actual_length():
        """
        """
        return len(self) + self._total_rowcount - self._fetched_rowcount

    def __getitem__(self, arg):
        """
        This behaves the exact same way as standard list indexing, except this allows indexing past the end of the list!
        This function will make a best effort to return data for the given index by fetching batches of snowflake
        records and accumulating them up to the index.

        Raises:
            TypeError: if the given arg isn't of type slice or int.
            IndexError: if the given arg represents a list index which isn't backed by snowflake data.
        """
        logger = get_logger()

        # First, determine the maximum index requested, which is what drives how many more batches to fetch from
        # snowflake.
        max_idx = None
        if isinstance(arg, slice):
            if arg.stop is None:
                max_idx = len(self) - 1
            else:
                max_idx = arg.stop
        elif isinstance(arg, int):
            max_idx = arg
        else:
            raise TypeError("list index must be of type slice or int.")

        # Next, fetch just the right amount of result batches to be able to index max_idx.  If there are no more results
        # to tap, then we'll raise an IndexError at this point.
        while max_idx >= len(self) and not self._results_exhausted:
            fetched_events = self._cursor.fetchmany(self._fetch_size)
            if fetched_events:
                self.extend(fetched_events)
                self._fetched_rowcount += len(fetched_events)
                if self._fetched_rowcount == self._total_rowcount:
                    self._results_exhausted = True
            else:
                # We should never get to this point, but if we have then something unexpected caused the number of
                # fetched rows to become exhausted before we fetch the expected "total" rowcount.  One thing that can
                # cause this to happen is if the caller manually fetched any results without letting this LazyResultFetcher
                # object do it.
                LOGGER.warning("Fetched fewer than expected results. Check for skipped events!")
                self._results_exhausted = True
        if max_idx >= len(self):
            raise IndexError("list index out of range, there are no more Snowflake results to fetch for this index.")

        # Finally, perform the actual indexing.
        return super(LazyResultFetcher, self).__getitem__(arg)


def _fetch_and_consolidate_and_batch(cursor, batch_size=1000, fetch_size=10000):
    """
    Given a snowflake cursor referencing the results of a query returning an entire processing segment of amplitude
    events, allocate events to workers such that the loading speed into amplitude is optimized.

    Argument `cursor` must not have had any rows fetched from it yet, and should represent the results of a query which:

    - Makes consecutive the events with the same user_id/device_id segment, i.e. coalesce(user_id, device_id).
    - Sorts user_id/device_id segments in descending order by rowcount of segments.
    - Sorts events by timestamp within each user_id/device_id segment.

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
        cursor (Snowflake cursor): Cursor of an executed query from which to fetch results.
        batch_size (int): Maximum number of events to include in a single Amplitude API call.  This cannot be greater
            than 1000 since that is currently the maximum imposed by Amplitude.
        fetch_size (int): The "small" number of events to fetch from snowflake each time we need to ask for more.

    Returns:
        iterable of iterable of iterable of events.
    """
    accumulated_events = LazyResultFetcher(cursor, fetch_size=fetch_size)
    last_idx_for_worker = None
    while True:
        if accumulated_events.actual_length() <= batch_size:
            # All we have left is one or more segments that completely fit into one batch, so send it all!
            last_idx_for_worker = len(accumulated_events)
        else:
            if accumulated_events[0].user_device == accumulated_events[batch_size - 1].user_device:
                # We have a segment which is larger than a single bucket.  We should find the last event for that
                # segment and send the entire segment to a worker.
                target_user_device = accumulated_events[0].user_device
                idx = batch_size - 1
                while True:
                    # Is there a more efficient way than indexing one event at a time?  Yes, but simple iteration is not
                    # our performance bottleneck, and anything more complicated can introduce bugs.
                    idx += 1
                    try:
                        if accumulated_events[idx].user_device != target_user_device:
                            last_idx_for_worker = idx
                            break
                    except IndexError:
                        last_idx_for_worker = idx
                        break
            else:
                # One or more segments completely fit into one batch, so lets consolidate them and send them.  Exclude
                # the last segment so that we send no more than one-batch-worth of events by finding the first event for
                # the last segment and using that as a cutoff:
                cut_user_device = accumulated_events[batch_size - 1].user_device
                idx = batch_size - 1
                while True:
                    idx -= 1
                    if accumulated_events[idx].user_device != cut_user_device:
                        last_idx_for_worker = idx + 1
                        break

        # Now we have `last_idx_for_worker` set to the point where we want to slice off events to send to a worker
        # thread.  Also, it is important that we delete that slice from the accumulated_events so that the python
        # garbage collector will free all that memory as soon as the corresponding worker thread returns.
        events_for_next_worker = accumulated_events[:last_idx_for_worker]
        del accumulated_events[:last_idx_for_worker]

        # Finally, batch the events such that each batch is small enough to fit inside a single Amplitude API call.
        yield batch_generator(events_for_next_worker, batch_size)


def _get_old_high_watermark(sf_credentials: SFCredentials, sf_role: str):
    """
    Get the amplitude event ID of the event immediately following the very last event sent to Amplitude's API.

    Raises:
        snowflake.connector.ProgrammingError: if the loader log table is inaccessible for some reason.
    """
    sf_connection = create_snowflake_connection(sf_credentials, sf_role)
    query = """
        select max(amplitude_event_id)
          from prod.amplitude_events.amplitude_events_loader_log
        """
    cursor.execute(query)
    old_high_watermark = cursor.fetchone()[0]
    if not old_high_watermark:
        old_high_watermark = 0


def _get_new_high_watermark(sf_credentials: SFCredentials, sf_role: str):
    """
    TODO: make this more robust (e.g. what if the table does not exist?  what if the table is empty?)
    """
    sf_connection = create_snowflake_connection(sf_credentials, sf_role)
    cursor = sf_connection.cursor()
    # Get the current maximum amplitude_event_id.
    query = """
        select max(amplitude_event_id)
          from prod.amplitude_events.amplitude_events
        """
    cursor.execute(query)
    return cursor.fetchone()[0]


def get_processing_segments():
    # Get the old (i.e. current) high watermark.
    old_high_watermark = _get_old_high_watermark()

    # Get the new high watermark, i.e. the current maximum amplitude_event_id.
    new_high_watermark = _get_new_high_watermark(sf_credentials, sf_role)

    # Get a list of processing segments which cover all events between the old and
    # new high watermark. The result is a list of start/end tuples describing
    # amplitude_event_id ranges, where each range represents a single processing
    # segment with the configured batch size.  The start ID is inclusive, whereas
    # the end ID is exclusive.
    #
    # e.g. if old=1000000, new=3600000, bucketsize=1000000, then:
    # processing_segments = [
    #     (1000000, 2000000),
    #     (2000000, 3000000),
    #     (3000000, 3600001),
    # ]
    #
    # In the above example, the last batch is smaller than 1m events because there
    # are no more events to include.
    return [
        (processing_segment_start, min(processing_segment_start + processing_bucket_size, new_high_watermark + 1))
        for processing_segment_start in range(old_high_watermark, new_high_watermark, processing_bucket_size)
    ]


def load_events_into_amplitude(amplitude_api_key: str, processing_segments: list)
    # Iterate over processing segments in chronological order, handling each one
    # one at a time, and completely freeing the memory of the previous one before
    # fetching the next.
    for processing_segment in processing_segments:
        # Prep a cursor to fetch the events for the current processing segment.
        #
        # For events with amplitude_event_id within the current processing_segment, further segment the events by the
        # following dimension:
        #
        #   coalesce(user_id, device_id)
        #
        # Then, sort events by segment size and sort by timestamp within each segment.
        segmented_by_device_user_query = (
            f"""
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
        )
        segmented_by_device_user = cursor.execute(segmented_by_device_user_query)

        # Make a generator which can fetch events and slice them for worker threads.
        segmented_by_device_user_and_batched = _fetch_and_consolidate_and_batch(cursor)

        # Only allow a fixed number (64, for example) of simultaneous network calls.
        available_senders = threading.Semaphore(64)

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
        def send_batch(amplitude_events_batch):
            """
            Actually make a Batch API request to send a batch of events.

            This is meant to be executed by a worker thread only.  Since we only want to allow a finite number of worker
            threads to be running API calls simultaneously, the API call is surrounded by logic to acquire/release a
            semaphore lock.  It's important that this semaphore logic exists *inside* the retry loop because we don't want
            to block any other workers while this one is in the middle of backing off.

            Args:
                amplitude_events_batch (iterable of events): 1000 or fewer events formatted for consumption by Amplitude.
            """
            available_senders.acquire()
            r = requests.post(
                "https://api2.amplitude.com/2/httpapi",
                json={
                    "api_key": amplitude_api_key,
                    "events": amplitude_events_batch,
                },
                headers=AMPLITUDE_REQUEST_HEADERS,
            )
            r.raise_for_status()
            available_senders.release()

        def load_device_segment(device_segment):
            """
            Load all the events for a given device within the current processing
            segment.

            Args:
              segment(iterable of iterable of events):
                All events for single user or device, batched by 1000.
            """
            for api_batch in device_segment:
                send_batch(api_batch)

        # Launch a thread pool of 128 (twice that of the semaphore limit) so that
        # we have a few extra threads to pick up the slack whenever a thread gets
        # stuck backing off due to EPDS rate limiting.  In theory, any time we have
        # fewer than the maximum number of blocking network calls, we are running
        # slower than capacity.
        with ThreadPoolExecutor(max_workers=128) as executor:
            executor.map(load_device_segment, segmented_by_device_user_and_batched)

        # Update the high watermark.
        # We do not need to wrap this in retry logic, since that is already
        # implemented in the prefect source:
        # https://github.com/PrefectHQ/prefect/blob/da07d255/src/prefect/client/client.py#L755-L756
        prefect.backend.set_key_value(
            key="amplitude_high_watermark",
            value=new_high_watermark
        )

        # This shouldn't be necessary, but make sure that we aren't deferring any garbage collection since the next
        # iteration may start to allocate a lot of memory.
        del segmented_by_device_user_and_batched
        gc.collect()
