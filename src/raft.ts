import { AppendEntriesRequest } from './append-entries-request';
import { AppendEntriesResponse } from './append-entries-response';
import { Log } from './log';
import { LogArrayHelper } from './log-array-helper';
import { RequestVoteRequest } from './request-vote-request';
import { RequestVoteResponse } from './request-vote-response';

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
    protected serverIds: Array<string>,
    protected sendAppendEntriesRequest: (
      id: string,
      appendEntriesRequest: AppendEntriesRequest
    ) => Promise<AppendEntriesResponse>,
    protected sendRequestVoteRequest: (
      requestVoteRequest: RequestVoteRequest
    ) => Promise<Array<RequestVoteResponse>>
  ) {
    setInterval(() => {
      this.applyToStateMachine();
    }, 1000);
  }

  public display() {
    console.log(
      `[${this.id}] state => ${this.state}, currentTerm: ${this.currentTerm}, [${this.log.length}]`
    );
  }

  protected async appendEntries(id: string): Promise<void> {
    if (LogArrayHelper.getLastIndex(this.log) < this.nextIndex[id]) {
      return;
    }

    const entries: Array<Log> = LogArrayHelper.slice(
      this.log,
      this.nextIndex[id]
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
      await this.sendAppendEntriesRequest(id, appendEntriesRequest);

    if (!appendEntriesResponse.success) {
      this.nextIndex[id] -= 1;

      return await this.appendEntries(id);
    }

    this.matchIndex[id] = LogArrayHelper.getLastIndex(entries);
    this.nextIndex[id] = LogArrayHelper.getTerm(
      entries,
      LogArrayHelper.getLastIndex(entries)
    );

    this.updateCommitIndex();
  }

  public applyToStateMachine(): void {
    for (let i = this.lastApplied + 1; i <= this.commitIndex; i++) {
      // TODO: applyToStateMachine
      const response = new Date().getTime();

      this.lastApplied = i;

      const callbacks = this.callbacks.filter((x) => x.index === i);

      for (const callback of callbacks) {
        callback.fn(null, response);

        // TODO: clean up
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
    appendEntriesRequest: AppendEntriesRequest
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
          : 0
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

    for (const serverId of this.serverIds.filter((x) => x !== this.id)) {
      await this.appendEntries(serverId);
    }

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
    requestVoteRequest: RequestVoteRequest
  ): RequestVoteResponse {
    if (requestVoteRequest.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }

    if (requestVoteRequest.term > this.currentTerm) {
      this.currentTerm = requestVoteRequest.term;
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

    for (const serverId of this.serverIds.filter((x) => x !== this.id)) {
      const appendEntriesRequest: AppendEntriesRequest = {
        entries: [],
        leaderCommit: this.commitIndex,
        leaderId: this.id,
        prevLogIndex: this.nextIndex[serverId] - 1,
        prevLogTerm: LogArrayHelper.getTerm(
          this.log,
          this.nextIndex[serverId] - 1
        ),
        term: this.currentTerm,
      };

      const appendEntriesResponse: AppendEntriesResponse =
        await this.sendAppendEntriesRequest(serverId, appendEntriesRequest);
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
        LogArrayHelper.getLastIndex(this.log)
      ),
      term: this.currentTerm,
    };

    const requestVoteResponses: Array<RequestVoteResponse> =
      await this.sendRequestVoteRequest(requestVoteRequest);

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
      Math.floor(this.serverIds.length / 2) + 1
    ) {
      return;
    }

    await this.toLeader();
  }

  protected toFollower(): void {
    this.state = 'follower';

    this.votedFor = null;
  }

  protected async toLeader(): Promise<void> {
    this.state = 'leader';

    this.votedFor = null;

    this.matchIndex = this.serverIds.reduce((dict, x) => {
      dict[x] = 0;

      return dict;
    }, {} as { [key: string]: number });

    this.nextIndex = this.serverIds.reduce((dict, x) => {
      dict[x] = LogArrayHelper.getLastIndex(this.log) + 1;

      return dict;
    }, {} as { [key: string]: number });

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
      const count: number = this.serverIds.filter(
        (x) => this.matchIndex[x] >= i
      ).length;

      if (
        count >= Math.floor(this.serverIds.length / 2) + 1 &&
        this.log[i - 1].term === this.currentTerm
      ) {
        this.commitIndex = i;
      }
    }
  }
}
