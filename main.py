import os
import shutil

from queue import Queue

from twophase.nodes import Node, MasterNode
from twophase.tasks import *

# Creating dictionary for storing nodes
nodes = {}


def create_node(node_id: int, vote_responses: dict, tasks: list, node_cls=Node):
    # initializing the new node (node_1)
    node_1 = node_cls(node_id, vote_responses, tasks)

    # loop through all current nodes (node_2) and connect them to this new node (node_1)
    for node_id2, node_2 in nodes.items():
        # creating 2 new queues as the channels between node_1 and node_2
        # node_1's in-channel would be node_2's out-channel and vice versa
        queue1 = Queue()    # node_1 -> node_2
        queue2 = Queue()    # node_2 -> node_1
        node_1.channel_add_in(node_id2, queue2)
        node_1.channel_add_out(node_id2, queue1)
        node_2.channel_add_in(node_id, queue1)
        node_2.channel_add_out(node_id, queue2)

    # add the new node to the node dictionary
    nodes[node_id] = node_1

    # start the new node thread
    node_1.start()

    return node_1


def link_failure(from_node, to_node, start_time, end_time):
    # t is start and end time for link failure
    t = (start_time, end_time)
    from_node.out_q_failure[to_node.node_id].append(t)
    to_node.in_q_failure[from_node.node_id].append(t)


def network_partition(node_group1, node_group2, start_time, end_time):
    for n1 in node_group1:
        for n2 in node_group2:
            link_failure(n1, n2, start_time, end_time)
            link_failure(n2, n1, start_time, end_time)


def stop():
    global nodes
    for node_id, node in nodes.items():
        node.terminate()
        node.join()

    nodes = {}


def main():
    if os.path.exists('logs'):
        shutil.rmtree('logs')
    os.mkdir('logs')
    FORMAT= '%(asctime)s, %(levelname)s: %(message)s'
    # logging the details for basic configuration
    logging.basicConfig(level=logging.INFO, format=FORMAT,datefmt="%Y-%m-%d %H:%M:%S")

    master = create_node(node_id=0,
                         vote_responses={0: VoteResponse(vote=1, delay=0), 1: VoteResponse(vote=1, delay=0)},
                         tasks=[SendVoteRequest(vote_id=0, time_to_execute=1),
                                # KillSelf(time_to_execute=2),
                                # ResumeSelf(time_to_execute=2.5),
                                SendVoteRequest(vote_id=1, time_to_execute=1.5)],
                         node_cls=MasterNode)
   # Creating node for participant1
    participant1 = create_node(node_id=1,
                               vote_responses={0: VoteResponse(vote=0, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                               tasks=[],
                               node_cls=Node)
    # Creating node for participant1
    participant2 = create_node(node_id=2,
                               vote_responses={0: VoteResponse(vote=1, delay=0.5), 1: VoteResponse(vote=1, delay=0.5)},
                               tasks=[],
                               node_cls=Node)

    # link_failure(master, participant1, 2, 5) and (master, participant2, 2, 5)
    network_partition([master, participant1], [participant2], 2, 5)


if __name__ == '__main__':
    main()
