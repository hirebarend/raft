import { Log } from './log';

export interface AppendEntriesRequest {
  entries: Array<Log>;

  leaderCommit: number;

  leaderId: string;

  prevLogIndex: number;

  prevLogTerm: number;

  term: number;
}
