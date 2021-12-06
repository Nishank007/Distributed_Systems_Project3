import os.path as osp
from threading import Thread

from .tasks import *


class Node(Thread):
    def __init__(self, node_id, vote_responses, tasks, freq=1000):
        super().__init__()
        self.node_id = node_id

        self.in_q = {}
        self.out_q = {}

        self.sleep = 1./freq
        self.stop = False
        self.start_time = time.time()
        self.vote_responses = vote_responses
        self.tasks = tasks

        # key: vote_id, value: status
        self.vote_status = {}
        # key: vote_id, value: (message, time-to-send)
        self.message_to_send = {}
        # key: vote_id, value: vote time
        self.pending_times = {}
        # key: node_id for in channel, value: a list of (start, end) time interval for link failure
        self.in_q_failure = {}
        # key: node_id for out channel, value: a list of (start, end) time interval for link failure
        self.out_q_failure = {}

        self.timeout = 2
        self.killed = False

    def terminate(self):
        self.stop = True

    def channel_add_in(self, node_id, queue):
        self.in_q[node_id] = queue
        self.in_q_failure[node_id] = list()

    def channel_add_out(self, node_id, queue):
        self.out_q[node_id] = queue
        self.out_q_failure[node_id] = list()

# discarding all messages in the channel if the channel currently fails, t[0] and t[1] are start and end time for link failure
    def receive_queue_failure(self, node_id):
        current_time = time.time() - self.start_time
        for t in self.in_q_failure[node_id]:
            if t[0] <= current_time <= t[1]:
                if self.in_q.get(node_id).qsize() > 0:
                    self.in_q.get(node_id).queue.clear()
                    logging.info(f'All messages from node {node_id} to {self.node_id} lost due to link failure')
                return

        if self.in_q.get(node_id).qsize() > 0:
            message = self.in_q.get(node_id).get()
            message.exec(self)

    def failures_enqueue(self, node_id, message):
        # discard the message if the channel currently fails, t[0] and t[1] are start and end time for link failure
        current_time = time.time() - self.start_time
        for t in self.out_q_failure[node_id]:
            if t[0] <= current_time <= t[1]:
                logging.info(f'Message lost: {message} from node {self.node_id} to {node_id} discarded due to link failure')
                return

        self.out_q.get(node_id).put(message)

# logging the votes on the disk using vote id , time
    def vote_log(self, time, vote_id, message):
        write_path = f'logs/{self.node_id}'
        write_mode = 'a' if osp.exists(write_path) else 'w'
        with open(write_path, write_mode) as file:
            file.write(f'{time}:{vote_id}:{message}\n')

    def recover(self):
        write_path = f'logs/{self.node_id}'
        with open(write_path, 'r') as file:
            # checking if self is the master node
            lines = file.readlines()
            for line in lines:
                _, vote_id, message = line.rstrip().split(':')
                if message == 'start':
                    self.recover_as_master(lines)
                    return

            # Recover as participant
            votes = {}
            statuses = {}

            for line in lines:
                _, vote_id, message = line.rstrip().split(':')
                if message in ['yes']:
                    votes[int(vote_id)] = message
                elif message in ['requested', 'commit', 'abort']:
                    statuses[int(vote_id)] = message

            for vote_id, status in statuses.items():
                # ignore the transactions that already have decisions
                if status in ['commit', 'abort']:
                    continue

                if vote_id in votes:
                    # uncertain - termination protocal
                    self.request_decision(int(vote_id))
                else:
                    # abort if there was no "yes" logged
                    current_time = time.time()
                    self.vote_log(current_time, vote_id, 'abort')
                    logging.info(f'Node {self.node_id} aborts transaction {vote_id}')
                    if vote_id in self.vote_status:
                        del self.vote_status[vote_id]

                    # Notify coordinator
                    self.vote_responses[vote_id].vote = 0
                    self.message_to_send[vote_id] = (Vote(self.node_id, vote_id, self.vote_responses[vote_id].vote),
                                                 time.time() + self.vote_responses[vote_id].delay)

    def recover_as_master(self, lines):
        raise NotImplementedError

    def prepare_vote(self, vote_id):
        response = self.vote_responses[vote_id]
        time_to_send = time.time() + response.delay
        self.message_to_send[vote_id] = (Vote(self.node_id, vote_id, response.vote), time_to_send)
        if response.vote:
            self.yes_vote(vote_id)
        else:
            self.no_vote(vote_id)

    def yes_vote(self, vote_id):
        self.vote_status[vote_id] = 'pending'
        logging.info(f'Node {self.node_id} votes YES for transaction {vote_id}, changing status to pending')
        current_time = time.time()
        self.vote_log(current_time, vote_id, 'yes')
        self.pending_times[vote_id] = current_time

    def no_vote(self, vote_id):
        self.vote_status[vote_id] = 'abort'
        logging.info(f'Node {self.node_id} votes NO for transaction {vote_id}')
        self.abort(vote_id)

    def commit(self, vote_id):
        current_time = time.time()
        self.vote_log(current_time, vote_id, 'commit')
        logging.info(f'Node {self.node_id} commits transaction {vote_id}')
        del self.vote_status[vote_id]

    def abort(self, vote_id):
        current_time = time.time()
        self.vote_log(current_time, vote_id, 'abort')
        logging.info(f'Node {self.node_id} aborts transaction {vote_id}')
        del self.vote_status[vote_id]

    def request_decision(self, vote_id):
        # check if the master hasn't sent the decision for too long using termination protocol
        logging.info(f'Node {self.node_id} timeout, initiate decision requests for transaction {vote_id}')
        for node_id, out_q in self.out_q.items():
            self.failures_enqueue(node_id, DecisionReq(vote_id, self.node_id))
        # reset timer for next decision request
        self.pending_times[vote_id] = time.time()

    def run(self):
        while True:
            if not self.killed:
                # receive from every node for the need of termination protocol
                for node_id, in_q in self.in_q.items():
                    self.receive_queue_failure(node_id)

                # check vote queue
                for vote_id, status in list(self.vote_status.items()):
                    if status == 'requested':
                        self.prepare_vote(vote_id)
                    elif status == 'commit':
                        self.commit(vote_id)
                    elif status == 'abort':
                        self.abort(vote_id)
                    elif status == 'pending' and time.time() > self.pending_times[vote_id] + self.timeout:
                        self.request_decision(vote_id)

            # check message_to_send list
            for vote_id, (message, time_to_send) in list(self.message_to_send.items()):
                current_time = time.time()
                if self.killed:
                    del self.message_to_send[vote_id]
                elif current_time >= time_to_send:
                    self.failures_enqueue(0, message)
                    del self.message_to_send[vote_id]

            # check tasks
            for i, task in enumerate(self.tasks):
                current_time = time.time()
                # execute task after at least (start time + time to execute)
                if current_time - self.start_time > task.time_to_execute:
                    task.exec(self)
                    self.tasks.remove(task)

            time.sleep(self.sleep)
            if self.stop:
                break


class MasterNode(Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ack_wait = list()
        self.votes = {}
        self.vote_req_times = {}
        self.jobs = list()
        self.timeout = 2

    def log_status(self, time, vote_id, status):
        write_path = f'logs/{self.node_id}'
        write_mode = 'a' if osp.exists(write_path) else 'w'
        with open(write_path, write_mode) as file:
            file.write(f'{time}:{vote_id}:{status}\n')

    def recover_as_master(self, lines):
        decisions = {}

        for line in lines:
            _, vote_id, message = line.rstrip().split(':')
            if message in ['start', 'commit', 'abort']:
                decisions[int(vote_id)] = message

        for vote_id, message in decisions.items():
            # abort if no decision has been made
            if message == 'start':
                self.vote_responses[vote_id] = 0
                self.votes[vote_id][0] = self.vote_responses[vote_id]
                current_time = time.time()
                self.log_status(current_time, vote_id, 'abort')
                for node, queue in self.out_q.items():
                    self.failures_enqueue(node, Abort(vote_id))
                if vote_id in self.votes:
                    del self.votes[vote_id]

    def run(self):
        while True:

            if not self.killed:
                # check response from participant nodes
                for node, queue in self.in_q.items():
                    self.receive_queue_failure(node)

                # collect messages from other nodes and decide to commit or abort
                for vote_id, votes in list(self.votes.items()):
                    abort = False
                    if not (-1 in votes):
                        # received all votes
                        if sum(votes) == len(votes):
                            current_time = time.time()
                            self.log_status(current_time, vote_id, 'commit')
                            # all vote YES
                            for node, queue in self.out_q.items():
                                self.failures_enqueue(node, Commit(vote_id))
                            logging.info(f'Master decides to commit transaction {vote_id}')
                            del self.votes[vote_id]
                        else:
                            abort = True
                            logging.info(f'Master aborts transaction {vote_id} due to partial agreement')
                    elif 0 in votes:
                        abort = True
                        logging.info(f'Master aborts transaction {vote_id} due to partial agreement')
                    elif time.time() > self.vote_req_times[vote_id] + self.timeout:
                        abort = True
                        logging.info(f'Master aborts transaction {vote_id} due to timeout')

                    if abort:
                        current_time = time.time()
                        self.log_status(current_time, vote_id, 'abort')
                        for node, queue in self.out_q.items():
                            self.failures_enqueue(node, Abort(vote_id))
                        del self.votes[vote_id]

            # check tasks
            for i, task in enumerate(self.tasks):
                current_time = time.time()
                if current_time - self.start_time > task.time_to_execute:
                    if not self.killed and isinstance(task, SendVoteRequest):
                        self.log_status(current_time, task.vote_id, 'start')
                        task.exec(self)
                        logging.info(f'Execute {task} at time {(time.time() - self.start_time) :.1f}s')
                        self.tasks.remove(task)
                    else:
                        # Kill or resume task
                        task.exec(self)
                        self.tasks.remove(task)

            time.sleep(self.sleep)
            if self.stop:
                break
