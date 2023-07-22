import * as uuid from 'uuid';
import { AppendEntriesRequest } from './append-entries-request';
import { Raft } from './raft';
import { RaftTransport } from './raft-transport';
import { RequestVoteRequest } from './request-vote-request';
import { StateMachine } from './state-machine';

class KeyValueStoreStateMachine implements StateMachine {
  protected dict: { [key: string]: string } = {};

  public async apply(command: {
    command: 'GET' | 'SET';
    key: string;
    value: string;
  }): Promise<any> {
    if (command.command === 'GET') {
      return this.dict[command.value] || null;
    }

    if (command.command === 'SET') {
      this.dict[command.value] = command.value;

      return 'OK';
    }

    return 'UNKNOWN';
  }
}

const stateMachine: StateMachine = new KeyValueStoreStateMachine();

const ids: Array<string> = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];

const rafts: Array<Raft> = [];

const raftTransports: Array<RaftTransport> = [];

for (const id of ids) {
  raftTransports.push({
    appendEntries: async (appendEntriesRequest: AppendEntriesRequest) => {
      const raft: Raft | undefined = rafts.find((x: Raft) => x.id === id);

      if (!raft) {
        throw new Error();
      }

      return await raft.handleAppendEntriesRequest(appendEntriesRequest);
    },
    getId: () => {
      return id;
    },
    requestVote: async (requestVoteRequest: RequestVoteRequest) => {
      const raft: Raft | undefined = rafts.find((x: Raft) => x.id === id);

      if (!raft) {
        throw new Error();
      }

      return await raft.handleRequestVote(requestVoteRequest);
    },
  });
}

for (const id of ids) {
  rafts.push(
    new Raft(
      id,
      raftTransports.filter((x) => x.getId() !== id),
      stateMachine,
    ),
  );
}

for (const raft of rafts) {
  setInterval(async () => {
    await raft.applyToStateMachine();
  }, 1000);

  cycle(50, 100, async () => {
    await raft.heartbeat();
  });

  cycle(100, 300, async () => {
    if (!raft.isLeader() && raft.electionTimeout()) {
      await raft.toCandidate();
    }
  });

  setInterval(async () => {
    raft.display();
  }, 2000);
}

async function cycle(
  min: number,
  max: number,
  fn: () => Promise<void>,
): Promise<void> {
  while (true) {
    await new Promise((resolve) =>
      setTimeout(
        async () => {
          resolve(null);
        },
        Math.random() * (max - min) + min,
      ),
    );

    await fn();
  }
}
