import { AppendEntriesRequest } from "./append-entries-request";
import { AppendEntriesResponse } from "./append-entries-response";
import { RequestVoteRequest } from "./request-vote-request";
import { RequestVoteResponse } from "./request-vote-response";

export interface RaftTransport {
    appendEntries(appendEntriesRequest: AppendEntriesRequest): Promise<AppendEntriesResponse>;

    getId(): string;

    requestVote: (
        requestVoteRequest: RequestVoteRequest
      ) => Promise<RequestVoteResponse>
}