import { AppendEntriesRequest } from './append-entries-request';
import { Raft } from './raft';
import { RequestVoteRequest } from './request-vote-request';

(async () => {
  const arr = ['a', 'b', 'c', 'd', 'e'];

  const raft: Array<Raft> = [];

  for (const x of arr) {
    raft.push(
      new Raft(
        x,
        arr,
        async (id: string, appendEntriesRequest: AppendEntriesRequest) => {
          const y = raft.find((y) => y.id === id);

          if (!y) {
            throw new Error();
          }

          return y.handleAppendEntriesRequest(appendEntriesRequest);
        },
        async (requestVoteRequest: RequestVoteRequest) => {
          return raft
            .filter((y) => y.id !== x)
            .map((y) => y.handleRequestVote(requestVoteRequest));
        }
      )
    );
  }

  for (const x of raft) {
    setInterval(async () => {
      await x.applyToStateMachine();
    }, 1000);

    cycle(50, 100, async () => {
      await x.heartbeat();
    });

    cycle(100, 300, async () => {
      if (!x.isLeader() && x.electionTimeout()) {
        await x.toCandidate();
      }
    });

    setInterval(async () => {
      x.display();
    }, 2000);
  }

  setInterval(async () => {
    for (const x of raft) {
      if (!x.isLeader()) {
        continue;
      }

      console.log(await x.handleRequest({ hello: 'world' }));
    }
  }, 1000);

  // setInterval(() => {
  //   for (const x of raft) {
  //     if (!x.isLeader()) {
  //       continue;
  //     }

  //     x.toFollower();
  //   }
  // }, 10000);

  // await raft[0].toCandidate();

  // await raft[1].toCandidate();

  // await raft[1].handleRequest('hello world');
  // await raft[1].handleRequest('foo bar');

  // await raft[0].toCandidate();

  // await raft[0].handleRequest('james smith');

  // for (const x of raft) {
  //   x.display();
  // }
})();

async function cycle(
  min: number,
  max: number,
  fn: () => Promise<void>
): Promise<void> {
  while (true) {
    await new Promise((resolve) =>
      setTimeout(async () => {
        resolve(null);
      }, Math.random() * (max - min) + min)
    );

    await fn();
  }
}
