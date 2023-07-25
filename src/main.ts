import axios from 'axios';
import * as hapi from '@hapi/hapi';
import { AppendEntriesRequest } from './append-entries-request';
import { Raft } from './raft';
import { RaftTransport } from './raft-transport';
import { RequestVoteRequest } from './request-vote-request';
import { StateMachine } from './state-machine';
import { AppendEntriesResponse } from './append-entries-response';
import { RequestVoteResponse } from './request-vote-response';

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

export class HttpRaftTransport implements RaftTransport {
  constructor(
    protected host: string,
    protected port: number,
  ) {}

  public async appendEntries(
    appendEntriesRequest: AppendEntriesRequest,
  ): Promise<AppendEntriesResponse> {
    const response = await axios.post(
      `http://${this.host}:${this.port}/rpc/append-entries`,
      appendEntriesRequest,
    );

    return response.data;
  }

  public getId(): string {
    return `${this.host}:${this.port}`;
  }

  public async requestVote(
    requestVoteRequest: RequestVoteRequest,
  ): Promise<RequestVoteResponse> {
    const response = await axios.post(
      `http://${this.host}:${this.port}/rpc/request-vote`,
      requestVoteRequest,
    );

    return response.data;
  }
}

(async () => {
  const ports = [8081, 8082, 8083];

  for (const port of ports) {
    const stateMachine: StateMachine = new KeyValueStoreStateMachine();

    const raft = new Raft(
      `127.0.0.1:${port}`,
      ports
        .filter((x) => x !== port)
        .map((x) => new HttpRaftTransport('127.0.0.1', x)),
      stateMachine,
    );

    const server = hapi.server({
      port,
      host: '0.0.0.0',
    });

    server.route({
      method: 'POST',
      path: '/rpc/append-entries',
      handler: async (request, h) => {
        return await raft.handleAppendEntriesRequest(request.payload as any);
      },
    });

    server.route({
      method: 'POST',
      path: '/rpc/request-vote',
      handler: async (request, h) => {
        return await raft.handleRequestVote(request.payload as any);
      },
    });

    await server.start();

    console.log('Server running on %s', server.info.uri);

    setTimeout(() => {
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
    }, 5000);
  }
})();

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
