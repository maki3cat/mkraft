
**FROM THIS DOC:**
https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md#protocol-ping-replica-replica

#### Inspirations and Followup-Questions:

1. **superblock**

the superblock seems to be a counterpart of our METASTATE, but
- the superblock which is not remoted replicated is stored with multipes sparsely on disk to insure resilience; 
this is a good idea, but my question is that the superblock/metastate seems be **changed constantly and synchronously with data plane requests** like (index changes) aside from election requests, will this kind of design impact the performance?
  - read the superblock to check what data changes trigger update, and how is the update implemeneted;
https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vsr/superblock.zig
  - add this to our design, and do benchmarking testing;


- why is that there are so many **invariants** and fields in the superblock than those in our metastate? how could they find so many invariants here?


2. **op-number and WAL** <- ! let me add this to mkraft asap>

- It seems every log has a monotonic number, and we don't have this, we use the index implicitly as indicated by the Raft paper; check if this matters;
- The WAL of tigerbeetle are two on-disk "ring-buffer"
  - probably one for header, one for payload, so that when we need headers only we can load the small ringbuffer faster
  - pre-allocate a fixed sized and maintain write-pointer and checkpoint-pointer to the positions of the file
    - need force checkpoint when write is faster than checkpoint?
    - the header-rb should be much smaller? to what ratio is scientific that triggers the fewest checkpoint?

3. What does this recovery refer to?
- is there a counterpart in RAFT

4. commands: why does this variate has so many command types??? 
- I need to add this client-ping/poing to get leader/term for mine
- what is this server-clock synchronization (by ping/ping) about?

5. different quorums?


RELATED READING

- https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf
