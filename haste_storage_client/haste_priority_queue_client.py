import logging

from haste_storage_client.core import LOGGING_FORMAT_DATE, LOGGING_FORMAT, HasteClient

from scipy.interpolate import interp1d
import numpy as np
import time

MODE_SPLINES = 0
MODE_NATURAL = 1
MODE_GOLDEN = 2

STATE_NONE = 0
STATE_IN_QUEUE_NOT_PRE_PROCESSED = 1
STATE_PRE_PROCESSING = 2
STATE_IN_QUEUE_PRE_PROCESSED = 3
STATE_POPPING = 4
STATE_POPPED = 5

BLOCK_SIZE = 15


# Requires Python 3.

class HastePriorityQueueClient(HasteClient):
    """
    A HASTE Client implemented as a priority queue. To save() a document is to push it to a queue for sending.
    Documents can also be locked for pre-processing. The interestingness values determine the priority for sending and pre-processing.
    """

    debug_fig_index = 0
    debug_count_files_preprocessed = 0

    def __init__(self,
                 capacity,
                 mode,
                 golden_estimated_interestingess_scores,
                 interestingess_model
                 ):
        logging.basicConfig(level=logging.INFO,
                            format=LOGGING_FORMAT,
                            datefmt=LOGGING_FORMAT_DATE)
        self.metadata = []
        self.mode = mode
        self.interestingess_model = interestingess_model

        # TODO: this should be the *initial* capacity.
        self.states = np.full(capacity, STATE_NONE)
        self.known_scores = np.full(capacity, -1)
        self.index = np.arange(0, capacity)
        self.index.flags['WRITEABLE'] = False

        # used for evaluation.
        self.golden_estimated_interestingness_scores = golden_estimated_interestingess_scores
        self.golden_estimated_interestingness_scores.flags['WRITEABLE'] = False

        logging.info(f'Mode is {["SPLINES", "NATURAL", "GOLDEN"][mode]}')

        # Use the ground truth as a baseline.

        if self.mode == MODE_GOLDEN:
            self.estimated_scores = self.golden_estimated_interestingness_scores.copy()
        else:
            self.estimated_scores = np.ones(capacity, dtype=np.float)

    def save(self,
             timestamp,
             location,
             substream_id,
             blob_bytes,
             metadata):
        """
        :param timestamp (numeric): should come from the cloud edge (eg. microscope). integer or floating point.
            *Uniquely identifies the document within the streaming session*.
        :param location (tuple): spatial information (eg. (x,y)).
        :param substream_id (string): ID for grouping of documents in stream (eg. microscopy well ID), or 'None'.
        :param blob_bytes (byte array): binary blob (eg. image).
        :param metadata (dict): extracted metadata (eg. image features).
        """
        self.metadata.append(metadata)
        index = len(self.metadata) - 1
        self.states[index] = STATE_IN_QUEUE_NOT_PRE_PROCESSED
        logging.info(f'PLOT_QUEUE - {time.time()} - NEW_FILE - {index}')

    def pop_for_sending(self):
        if np.sum(self.states == STATE_IN_QUEUE_PRE_PROCESSED) > 0:
            logging.debug('Send the first preprocessed file:')
            index_to_send = np.argmax(self.states == STATE_IN_QUEUE_PRE_PROCESSED)
        elif np.sum(self.states == STATE_IN_QUEUE_NOT_PRE_PROCESSED) == 0:
            logging.info('no files to send at the moment')
            index_to_send = None
        elif self.mode == MODE_SPLINES or self.mode == MODE_GOLDEN:
            # We're trying to send the one with the lowest overall score, in the state STATE_UNSENT_NOT_PRE_PROCESSED
            est_scores = self.estimated_scores.copy()
            est_scores[self.states != STATE_IN_QUEUE_NOT_PRE_PROCESSED] = np.nan
            index_to_send = np.nanargmin(est_scores)
        elif self.mode == MODE_NATURAL:
            # Note, False < True...  -- get the first true index
            # (we checked that we definitely had one to send at the top)
            index_to_send = np.argmax(self.states == STATE_IN_QUEUE_NOT_PRE_PROCESSED)
        else:
            raise Exception(f'mode {self.mode} not known')

        if index_to_send is None:
            return None, None
        logging.info(f'PLOT_QUEUE - {time.time()} - POP_SEND_PRE - {index_to_send}')
        self.states[index_to_send] = STATE_POPPING
        return index_to_send, self.metadata[index_to_send]

    def next_to_preprocess(self):
        unprocessed_files = self.states == STATE_IN_QUEUE_NOT_PRE_PROCESSED

        if np.sum(unprocessed_files) == 0:
            logging.info('No files to preprocess')
            index_to_process = None
        elif self.mode == MODE_SPLINES:
            index_to_process = None

            # first, search any blocks where we we haven't searched already:
            for block_start in range(0, len(self.metadata), BLOCK_SIZE):
                block_end_excl = min(block_start + BLOCK_SIZE, len(self.metadata))

                logging.info(f'block start,end_excl: {block_start, block_end_excl}')

                none_in_block_available_for_preprocessing = not np.any(
                    self.states[block_start:block_end_excl] == STATE_IN_QUEUE_NOT_PRE_PROCESSED)
                some_in_block_already_known_score = np.any(self.known_scores[block_start:block_end_excl] >= 0) or np.any(
                    self.states[block_start:block_end_excl] == STATE_PRE_PROCESSING)

                if none_in_block_available_for_preprocessing or some_in_block_already_known_score:
                    continue
                else:
                    index_to_process = block_start + np.argmax(
                        self.states[block_start:block_end_excl] == STATE_IN_QUEUE_NOT_PRE_PROCESSED)
                    logging.info(f'PLOT_QUEUE - {time.time()} - POP_PREPROCESS_SEARCH - {index_to_process}')
                    break

            if index_to_process is None:
                # 'climb'
                est_scores = self.estimated_scores.copy()
                est_scores[self.states != STATE_IN_QUEUE_NOT_PRE_PROCESSED] = np.nan
                index_to_process = np.nanargmax(est_scores)
                logging.info(f'PLOT_QUEUE - {time.time()} - POP_PREPROCESS - {index_to_process}')


        elif self.mode == MODE_GOLDEN:
            # 'climb'
            est_scores = self.estimated_scores.copy()
            est_scores[self.states != STATE_IN_QUEUE_NOT_PRE_PROCESSED] = np.nan
            index_to_process = np.nanargmax(est_scores)
            logging.info(f'PLOT_QUEUE - {time.time()} - POP_PREPROCESS - {index_to_process}')

        elif self.mode == MODE_NATURAL:
            # Send next unprocessed file
            index_to_process = np.argmax(unprocessed_files)
            logging.info(f'PLOT_QUEUE - {time.time()} - POP_PREPROCESS - {index_to_process}')
        else:
            raise Exception('mode not known')

        if index_to_process is None:
            return None, None

        self.debug_count_files_preprocessed += 1
        self.states[index_to_process] = STATE_PRE_PROCESSING
        return index_to_process, self.metadata[index_to_process]

    def notify_popped(self, index):
        assert self.states[index] == STATE_POPPING
        self.states[index] = STATE_POPPED

    def notify_preprocessed(self, index, interestingness_score, new_metadata):
        assert self.states[index] == STATE_PRE_PROCESSING
        self.states[index] = STATE_IN_QUEUE_PRE_PROCESSED
        self.known_scores[index] = interestingness_score
        self.metadata[index] = new_metadata

        # The interestingness models used for the Tiered Storage systems use an interestingness function which is a function of a single document.
        # In this case, the interestingness of all documents depends on all other documents. 
        # We use the same API.
        # TODO: think about creating a uniform API for interestingness functions of both kinds. 

        self.interestingess_model.interestingness(None,  # stream_id
                                                  None,  # timestamp
                                                  index,  # location
                                                  None,  # substream ID
                                                  new_metadata,  # metadata
                                                  self)  # context collection

    def log_queue_info(self):
        # Log info about the present state of the queue
        count_preprocessed = np.sum(self.states == STATE_IN_QUEUE_PRE_PROCESSED)
        count_not_preprocessed = np.sum(self.states == STATE_IN_QUEUE_NOT_PRE_PROCESSED)
        logging.info(f'PLOT - {time.time()} - {count_preprocessed} - {count_not_preprocessed}')

    # def plot(self):
    #     # plt.plot(self.index, map(lambda filename: get_golden_prio_for_filename(filename), )
    #
    #     plt.plot(self.index, self.estimated_scores)
    #     plt.savefig(f'figures/0.splines.{self.debug_fig_index}.png')
    #     self.debug_fig_index += 1


if __name__ == '__main__':
    import random

    mq = HastePriorityQueueClient(20)
    for i in range(0, 20):
        mq.push(f'file_{i}')

    for i in range(20):
        index, filepath = mq.next_to_preprocess()
        assert index is not None
        score = 15 + 5 * np.cos(index / 10 * 2 * np.pi) + random.randint(-2, 2)

        mq.notify_preprocessed(index, score, f'new_file_{i}')

    for i in range(10):
        mq.pop_for_sending()
