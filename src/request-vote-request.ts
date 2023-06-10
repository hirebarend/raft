export interface RequestVoteRequest{
    candidateId: string;

    lastLogIndex: number;

    lastLogTerm: number;

    term: number;
}