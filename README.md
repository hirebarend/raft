# Raft

TypeScript implementation of the Raft consensus protocol

## Example

```typescript
import { Raft } from 'raft';

const raft: Raft = new Raft(
  '192.0.0.1',
  ['192.0.0.1', '192.0.0.2', '192.0.0.3', '192.0.0.4'],
  async (id: string, appendEntriesRequest: AppendEntriesRequest) => {
    throw new Error('not implemented yet');
  },
  async (requestVoteRequest: RequestVoteRequest) => {
    throw new Error('not implemented yet');
  },
  new StateMachine()
);
```
