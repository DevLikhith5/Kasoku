# Kasoku Complete UML & Sequence Diagrams

This document contains Mermaid UML diagrams representing the core architecture and complex workflows of Kasoku.

## 1. Storage Engine (LSM-Tree) Architecture
*Shows the physical storage layout on a single node.*
```mermaid
classDiagram
    class Engine {
        -config Config
        -wal *WAL
        -memTable *MemTable
        -immutableMemTables []*MemTable
        -sstables [][]*SSTable
        -compactor *Compactor
        +Put(key string, value []byte) error
        +Get(key string) (Entry, error)
        +Flush() error
    }

    class MemTable {
        -skipList *SkipList
        +Put(key string, value []byte, version uint64)
        +Get(key string) (Entry, bool)
    }

    class WAL {
        -file *os.File
        +Append(entry Entry) error
        +Sync() error
    }

    class SSTable {
        -file *os.File
        -bloomFilter *BloomFilter
        -index map[string]int64
        +Get(key string) (Entry, bool)
    }

    Engine "1" *-- "1" WAL : Persists to
    Engine "1" *-- "1" MemTable : Writes to
    Engine "1" *-- "*" SSTable : Flushes to
```

## 2. Distributed Cluster Architecture
*Shows the network layer and internal cluster services.*
```mermaid
classDiagram
    class Cluster {
        -nodeID string
        -ring *Ring
        -members *MemberList
        -rpcPool *rpc.Pool
        -replicator *BackgroundReplicator
        +Put(key string, value []byte) error
    }

    class Ring {
        -vnodes []int
        -nodeMap map[int]string
        +GetNodes(key string, count int) []string
    }

    class MemberList {
        -members map[string]*Member
        +Merge(remoteMembers []string)
        +Gossip()
    }

    class Pool {
        -clients map[string][]*ReplicatedClient
        +Get(addr string) *ReplicatedClient
    }

    Cluster "1" *-- "1" Ring : Routes requests via
    Cluster "1" *-- "1" MemberList : Tracks health via
    Cluster "1" *-- "1" Pool : Manages network via
```

## 3. Standard Write Flow (Quorum=2)
*Shows a successful parallel write to multiple nodes.*
```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant Replica1
    participant Replica2

    Client->>Coordinator: PUT user:1
    Coordinator->>Coordinator: Hash "user:1" -> [Coordinator, Replica1, Replica2]
    
    par Parallel Replication
        Coordinator->>Coordinator: Local WAL & MemTable Put
    and
        Coordinator->>Replica1: gRPC Put(user:1)
    and
        Coordinator->>Replica2: gRPC Put(user:1)
    end
    
    Replica1-->>Coordinator: Ack
    Note over Coordinator: Received 2 Acks (Quorum met)
    Coordinator-->>Client: 200 OK
    Replica2-->>Coordinator: Ack (arrives late, ignored)
```

## 4. Failure Recovery: Hinted Handoff
*Shows what happens when a node crashes during a write.*
```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant FallbackNode
    participant CrashedReplica

    Note over CrashedReplica: Node is offline
    
    Client->>Coordinator: PUT user:2
    Coordinator->>CrashedReplica: gRPC Put(user:2)
    CrashedReplica--xCoordinator: Connection Refused / Timeout
    
    Note over Coordinator: Cannot reach primary replica!
    
    Coordinator->>FallbackNode: gRPC Put(user:2) WITH HINT (Target: CrashedReplica)
    FallbackNode->>FallbackNode: Save Data + Save Hint to Disk
    FallbackNode-->>Coordinator: Ack
    Coordinator-->>Client: 200 OK
    
    Note over CrashedReplica: Node boots back up (1 hour later)
    
    loop Every 10 Seconds
        FallbackNode->>CrashedReplica: Check if alive
        FallbackNode->>CrashedReplica: gRPC Put(user:2) [Deliver Hint]
        CrashedReplica-->>FallbackNode: Ack
        FallbackNode->>FallbackNode: Delete Hint from Disk
    end
```

## 5. Background Consistency: Gossip Protocol
*Shows how the cluster detects dead nodes without a master server.*
```mermaid
sequenceDiagram
    participant NodeA
    participant NodeB
    participant NodeC

    loop Every 500ms
        NodeA->>NodeA: Pick random peer (NodeB)
        NodeA->>NodeB: UDP: "I know A=Alive, B=Alive"
        NodeB->>NodeB: Merge state
        
        NodeB->>NodeB: Pick random peer (NodeC)
        NodeB->>NodeC: UDP: "I know A=Alive, B=Alive, C=Alive"
        NodeC->>NodeC: Merge state
        
        Note over NodeC: If Node A dies, Node A's heartbeat stops.<br/>After 5 seconds, NodeB marks A=Dead.<br/>Gossip spreads A=Dead to all nodes exponentially.
    end
```

## 6. Storage Engine: LSM Compaction
*Shows how the background compactor cleans up the physical disk to prevent reads from slowing down.*
```mermaid
flowchart TD
    MemTable["Active MemTable (RAM)"]
    ImMemTable["Immutable MemTable (RAM)"]
    L0["Level 0 SSTables (Disk)"]
    L1["Level 1 SSTables (Disk)"]
    L2["Level 2 SSTables (Disk)"]
    
    MemTable -- "16MB Limit Reached" --> ImMemTable
    ImMemTable -- "Flush to Disk" --> L0
    
    L0 -- "Compaction (Merge & Sort)" --> L1
    L1 -- "Compaction (Merge & Sort)" --> L2
    
    Note1["L0: Files can have overlapping keys"]
    Note2["L1+: Files have strictly non-overlapping keys"]
    
    L0 -.-> Note1
    L1 -.-> Note2
```

## 7. Anti-Entropy: Merkle Tree Sync
*Shows how nodes silently fix missing data using cryptographic hashes.*
```mermaid
sequenceDiagram
    participant NodeA
    participant NodeB

    loop Every 60 seconds
        NodeA->>NodeA: Build Merkle Tree of all local keys
        NodeB->>NodeB: Build Merkle Tree of all local keys
        
        NodeA->>NodeB: Exchange Root Hash
        
        alt Root Hashes Match
            Note over NodeA,NodeB: Data is 100% in sync. Do nothing.
        else Root Hashes Differ
            NodeA->>NodeB: Compare Level 1 Hashes
            Note over NodeA,NodeB: Binary search down the tree until the exact mismatched leaf is found
            NodeA->>NodeB: Send missing data for mismatched key only
        end
    end
```

## 8. Version Control: Vector Clocks
*Shows how Kasoku handles conflicting writes from different nodes without a master server.*
```mermaid
sequenceDiagram
    participant Client
    participant NodeA
    participant NodeB

    Client->>NodeA: PUT name="Alice"
    NodeA->>NodeA: Assign Vector Clock: [A:1]
    Note over NodeA: Network Partition! NodeA cannot talk to NodeB.
    
    Client->>NodeB: PUT name="Bob"
    NodeB->>NodeB: Assign Vector Clock: [B:1]
    
    Note over NodeA,NodeB: Network Heals. Read requested.
    Client->>NodeA: GET name
    NodeA->>NodeB: Fetch Replica
    NodeB-->>NodeA: Returns "Bob" [B:1]
    
    Note over NodeA: Compare [A:1] vs [B:1].<br/>Conflict Detected! No clear winner.
    NodeA-->>Client: Returns BOTH: ["Alice", "Bob"]
    Note over Client: Client application must resolve the conflict and write back the winner.
```

## 9. Self-Healing: Read Repair
*Shows how the database fixes stale data on the fly during a user's read request.*
```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant FastReplica
    participant SlowReplica

    Client->>Coordinator: GET user:1 (R=2)
    
    par Parallel Reads
        Coordinator->>FastReplica: Get(user:1)
        Coordinator->>SlowReplica: Get(user:1)
    end
    
    FastReplica-->>Coordinator: Returns "Alice_v2" [A:2]
    SlowReplica-->>Coordinator: Returns "Alice_v1" [A:1]
    
    Note over Coordinator: Identifies SlowReplica is out of date.
    
    Coordinator-->>Client: Returns "Alice_v2"
    
    Note over Coordinator: Async Read Repair triggered in background
    Coordinator->>SlowReplica: Push "Alice_v2" [A:2]
    SlowReplica->>SlowReplica: Overwrites old data
```
