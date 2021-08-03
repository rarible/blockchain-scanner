### Blockchain scanner library core

This is a base for creating blockchain scanners. Scanner is a tool that listens to blockchain events:

- listens to new blocks
- checks if reorgs are performed
- listens to specific events (for Ethereum, for example by topic) from found blocks
- allows to extract additional data for events from the blockchain (and saves this data alongside with events)
- reindexing (when first started or need to reindex data in case of error)