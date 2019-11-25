import sys

import numpy as np
from haste_storage_client.haste_priority_queue_client import MODE_GOLDEN, HastePriorityQueueClient, STATE_NONE, STATE_IN_QUEUE_NOT_PRE_PROCESSED, STATE_PRE_PROCESSING, STATE_POPPING, STATE_POPPED, \
    STATE_IN_QUEUE_PRE_PROCESSED


def test_master_queue_golden():
    if sys.version_info[0] == 2 or sys.version_info[1] < 6:
        # MasterQueue only supports Python 3.6 or above.
        # TODO: investigate fixing this.
        return

    class MockInterestingnessModel:
        def interestingness(self,
                            stream_id=None,
                            timestamp=None,
                            location=None,
                            substream_id=None,
                            metadata=None,
                            mongo_collection=None):
            pass

    q = HastePriorityQueueClient(5, MODE_GOLDEN, np.array([1, 2, 3, 4, 5], dtype=np.float), interestingess_model=MockInterestingnessModel())
    assert ((q.states == STATE_NONE).all())

    for i in range(5):
        q.save(1,
               1,
               None,
               None,
               {})

    assert ((q.states == STATE_IN_QUEUE_NOT_PRE_PROCESSED).all())

    ###

    index, _ = q.next_to_preprocess()
    assert (index == 4)
    assert (q.states[4] == STATE_PRE_PROCESSING)

    index, _ = q.pop_for_sending()
    assert (index == 0)
    assert (q.states[0] == STATE_POPPING)

    q.notify_preprocessed(4, 1, {})
    q.notify_popped(0)

    assert np.all(q.states == np.array([
        STATE_POPPED,
        STATE_IN_QUEUE_NOT_PRE_PROCESSED,
        STATE_IN_QUEUE_NOT_PRE_PROCESSED,
        STATE_IN_QUEUE_NOT_PRE_PROCESSED,
        STATE_IN_QUEUE_PRE_PROCESSED
    ]))

    ###

    index, _ = q.next_to_preprocess()
    assert (index == 3)
    assert (q.states[3] == STATE_PRE_PROCESSING)

    index, _ = q.pop_for_sending()
    assert (index == 4)
    assert (q.states[4] == STATE_POPPING)

    q.notify_preprocessed(3, 1, {})
    q.notify_popped(4)

    assert np.all(q.states == np.array([
        STATE_POPPED,
        STATE_IN_QUEUE_NOT_PRE_PROCESSED,
        STATE_IN_QUEUE_NOT_PRE_PROCESSED,
        STATE_IN_QUEUE_PRE_PROCESSED,
        STATE_POPPED
    ]))
