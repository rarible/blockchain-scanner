# Blockchain Scanner Library Core

This is a base for creating blockchain scanners. Scanner is a tool that listens to blockchain events:

- Listens to new blocks
- Checks if reorgs are performed
- Listens to specific events (for Ethereum by topic, for example) from found blocks
- Allows extracting additional data for events from the blockchain (and saves this data alongside with events)
- Reindexing (when first started or need to reindex data in case of error)

## Suggestions

You are welcome to [suggest features](https://github.com/rarible/protocol/discussions) and [report bugs found](https://github.com/rarible/protocol/issues)!

## Contributing

The codebase is maintained using the "contributor workflow" where everyone without exception contributes patch proposals using "pull requests" (PRs). This facilitates social contribution, easy testing, and peer review.

See more information on [CONTRIBUTING.md](https://github.com/rarible/protocol/blob/main/CONTRIBUTING.md).

## License

Blockchain Scanner Library Core is available under the [MIT License](LICENSE.md).