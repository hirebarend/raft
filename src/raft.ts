import { AppendEntriesRequest } from './append-entries-request';
import { AppendEntriesResponse } from './append-entries-response';
import { Log } from './log';
import { LogArrayHelper } from './log-array-helper';
import { RaftTransport } from './raft-transport';
import { RequestVoteRequest } from './request-vote-request';
import { RequestVoteResponse } from './request-vote-response';
import { StateMachine } from './state-machine';

export class Raft {
  // <PERSISTENT STATE>
  protected currentTerm: number = 0;

  protected log: Array<Log> = [];

  protected votedFor: string | null = null;
  // </PERSISTENT STATE>

  // <VOLATILE STATE>
  protected commitIndex: number = 0;

  protected lastApplied: number = 0;
  // </VOLATILE STATE>

  // <VOLATILE STATE FOR LEADER>
  protected matchIndex: { [key: string]: number } = {};

  protected nextIndex: { [key: string]: number } = {};
  // </VOLATILE STATE FOR LEADER>

  protected callbacks: Array<{
    index: number;
    fn: (error: Error | null, value: any) => void;
  }> = [];

  public lastAppendEntriesTimestamp: number | null = null;

  protected state: 'candidate' | 'follower' | 'leader' | 'terminated' =
    'follower';

  constructor(
    public id: string,
    protected raftTransports: Array<RaftTransport>,
    protected stateMachine: StateMachine,
  ) {}

  public display() {
    console.log(
      `[${this.id}] state => ${this.state}, currentTerm: ${this.currentTerm}, [${this.log.length}]`,
    );
  }

  protected async appendEntries(raftTransport: RaftTransport): Promise<void> {
    const id: string = raftTransport.getId();

    if (LogArrayHelper.getLastIndex(this.log) < this.nextIndex[id]) {
      return;
    }

    const entries: Array<Log> = LogArrayHelper.slice(
      this.log,
      this.nextIndex[id],
    );

    const appendEntriesRequest: AppendEntriesRequest = {
      entries,
      leaderCommit: this.commitIndex,
      leaderId: this.id,
      prevLogIndex: entries[0].index - 1,
      prevLogTerm: LogArrayHelper.getTerm(this.log, entries[0].index - 1),
      term: this.currentTerm,
    };

    const appendEntriesResponse: AppendEntriesResponse =
      await raftTransport.appendEntries(appendEntriesRequest);

    if (!appendEntriesResponse.success) {
      this.nextIndex[id] -= 1;

      return await this.appendEntries(raftTransport);
    }

    this.matchIndex[id] = LogArrayHelper.getLastIndex(entries);
    this.nextIndex[id] = LogArrayHelper.getTerm(
      entries,
      LogArrayHelper.getLastIndex(entries),
    );

    this.updateCommitIndex();
  }

  public async applyToStateMachine(): Promise<void> {
    for (let i = this.lastApplied + 1; i <= this.commitIndex; i++) {
      const response = await this.stateMachine.apply(this.log[i].data);

      this.lastApplied = i;

      const callbacks = this.callbacks.filter((x) => x.index === i);

      for (const callback of callbacks) {
        callback.fn(null, response);

        this.callbacks.splice(this.callbacks.indexOf(callback), 1);
      }
    }
  }

  public electionTimeout(): boolean {
    // TODO
    return (
      this.lastAppendEntriesTimestamp === null ||
      new Date().getTime() - this.lastAppendEntriesTimestamp > 300
    );
  }

  public handleAppendEntriesRequest(
    appendEntriesRequest: AppendEntriesRequest,
  ): AppendEntriesResponse {
    this.lastAppendEntriesTimestamp = new Date().getTime();

    if (this.state !== 'follower') {
      this.toFollower();
    }

    if (this.votedFor) {
      this.votedFor = null;
    }

    if (appendEntriesRequest.term < this.currentTerm) {
      return {
        success: false,
        term: this.currentTerm,
      };
    }

    if (appendEntriesRequest.term > this.currentTerm) {
      this.currentTerm = appendEntriesRequest.term;
    }

    if (
      LogArrayHelper.getTerm(this.log, appendEntriesRequest.prevLogIndex) !==
      appendEntriesRequest.prevLogTerm
    ) {
      return {
        success: false,
        term: this.currentTerm,
      };
    }

    for (const entry of appendEntriesRequest.entries) {
      if (
        this.log[entry.index - 1] &&
        LogArrayHelper.getTerm(this.log, entry.index) != entry.term
      ) {
        this.log = this.log.slice(entry.index);
      }

      this.log.push(entry);
    }

    if (appendEntriesRequest.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(
        appendEntriesRequest.leaderCommit,
        appendEntriesRequest.entries.length
          ? appendEntriesRequest.entries[
              appendEntriesRequest.entries.length - 1
            ].index
          : 0,
      );
    }

    return {
      success: true,
      term: this.currentTerm,
    };
  }

  public async handleRequest(data: any): Promise<any> {
    if (!this.isLeader()) {
      throw new Error();
    }

    const log: Log = {
      data,
      index: LogArrayHelper.getLastIndex(this.log) + 1,
      term: this.currentTerm,
    };

    this.log.push(log);

    await Promise.all(
      this.raftTransports.map((raftTransport: RaftTransport) =>
        this.appendEntries(raftTransport),
      ),
    );

    return await new Promise((resolve, reject) => {
      this.callbacks.push({
        index: log.index,
        fn: (error: Error | null, value: any) => {
          if (error) {
            reject(error);

            return;
          }

          resolve(value);
        },
      });
    });
  }

  public handleRequestVote(
    requestVoteRequest: RequestVoteRequest,
  ): RequestVoteResponse {
    if (requestVoteRequest.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }

    if (requestVoteRequest.term > this.currentTerm) {
      this.currentTerm = requestVoteRequest.term;

      this.toFollower(); // ??
    }

    if (this.votedFor && this.votedFor !== requestVoteRequest.candidateId) {
      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }

    if (
      requestVoteRequest.lastLogIndex < LogArrayHelper.getLastIndex(this.log)
    ) {
      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }

    this.votedFor = requestVoteRequest.candidateId;

    return {
      term: this.currentTerm,
      voteGranted: true,
    };
  }

  public async heartbeat(): Promise<void> {
    if (!this.isLeader()) {
      return;
    }

    for (const raftTransport of this.raftTransports) {
      const id: string = raftTransport.getId();

      const appendEntriesRequest: AppendEntriesRequest = {
        entries: [],
        leaderCommit: this.commitIndex,
        leaderId: this.id,
        prevLogIndex: this.nextIndex[id] - 1,
        prevLogTerm: LogArrayHelper.getTerm(this.log, this.nextIndex[id] - 1),
        term: this.currentTerm,
      };

      const appendEntriesResponse: AppendEntriesResponse =
        await raftTransport.appendEntries(appendEntriesRequest);
    }
  }

  public isLeader(): boolean {
    return this.state === 'leader';
  }

  public async toCandidate(): Promise<void> {
    this.state = 'candidate';

    this.currentTerm += 1;

    this.votedFor = this.id;

    const requestVoteRequest: RequestVoteRequest = {
      candidateId: this.id,
      lastLogIndex: LogArrayHelper.getLastIndex(this.log),
      lastLogTerm: LogArrayHelper.getTerm(
        this.log,
        LogArrayHelper.getLastIndex(this.log),
      ),
      term: this.currentTerm,
    };

    const requestVoteResponses: Array<RequestVoteResponse> = await Promise.all(
      this.raftTransports.map((raftTransport: RaftTransport) =>
        raftTransport.requestVote(requestVoteRequest),
      ),
    );

    for (const requestVoteResponse of requestVoteResponses) {
      if (requestVoteResponse.term > this.currentTerm) {
        this.currentTerm = requestVoteResponse.term;

        this.toFollower();
      }
    }

    if (this.state !== 'candidate') {
      return;
    }

    if (
      requestVoteResponses.filter((x) => x.voteGranted).length + 1 <
      Math.floor((this.raftTransports.length + 1) / 2) + 1
    ) {
      return;
    }

    await this.toLeader();
  }

  protected toFollower(): void {
    this.state = 'follower';

    this.lastAppendEntriesTimestamp = new Date().getTime(); // ??

    this.votedFor = null;
  }

  protected async toLeader(): Promise<void> {
    this.state = 'leader';

    this.votedFor = null;

    this.matchIndex = this.raftTransports.reduce(
      (dict, raftTransport: RaftTransport) => {
        const id: string = raftTransport.getId();

        dict[id] = 0;

        return dict;
      },
      {} as { [key: string]: number },
    );

    this.matchIndex[this.id] = 0;

    this.nextIndex = this.raftTransports.reduce(
      (dict, raftTransport: RaftTransport) => {
        const id: string = raftTransport.getId();

        dict[id] = LogArrayHelper.getLastIndex(this.log) + 1;

        return dict;
      },
      {} as { [key: string]: number },
    );

    this.nextIndex[this.id] = LogArrayHelper.getLastIndex(this.log) + 1;

    await this.heartbeat();
  }

  public terminate(): void {
    this.state = 'terminated';
  }

  protected updateCommitIndex() {
    for (
      let i = this.commitIndex + 1;
      i <= LogArrayHelper.getLastIndex(this.log);
      i++
    ) {
      const count: number = Object.keys(this.matchIndex).filter(
        (key: string) => this.matchIndex[key] >= i,
      ).length;

      if (
        count >= Math.floor((this.raftTransports.length + 1) / 2) + 1 &&
        this.log[i - 1].term === this.currentTerm
      ) {
        this.commitIndex = i;
      }
    }
  }
}
