Assignment2

Requirements:
	-Python3.7


Implementation:
1. The coordinator sends in the vote request message to participants and will write a log to disk to record the starting of the transaction.
2. When a vote request is recieved by the participant, it responds by sending a message (YES or NO) to the coordinator.
3. NO will be recorded as ABORT.
4. The coordinator will collect all vote messages from the participants. If all of them are YES READY and the coordinator’s vote is also Yes, then the coordinator decides Commit and sends COMMIT messages to all participants.
5. The coordinator decides Abort and sends ABORT messages to all participants if its the other case.
6. The participants that voted Yes waits for a COMMIT or ABORT message from the coordinator. When it receives the message, it decides accordingly and stops.

1. 'twophase/nodes.py' has participant and coordinator logic for two phase protocol, logging and the termination protocol logic.
2. 'twophase/messages.py' implements functions Vote, Commit, Abort, VoteReq, DecisionReq.
3. 'twophase/tasks.py' implements tasks such as node crashes and recovery.
4. Run 'python3 main.py' in one of the windows.

### Run Tests
To run the tests, run 'python3 -m tests -v'


References:-
http://www.mathcs.emory.edu/~cheung/Courses/554/Syllabus/9-parallel/2-phase.html
https://shekhargulati.com/2018/09/05/two-phase-commit-protocol/
https://www.cs.princeton.edu/courses/archive/fall16/cos418/docs/L6-2pc.pdf
https://people.csail.mit.edu/alinush/6.824-spring-2015/l20-argus.html
