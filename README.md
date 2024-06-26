![ByteVault](/screenshots/ByteVault.png)

# Bytevault

Bytevault is a high-performance, disk-based key-value store inspired by Bitcask. It is designed for efficient data handling with fast read and write operations, ensuring data integrity during concurrent access. Bytevault is built to handle large datasets with efficient indexing and offers reliable, crash-friendly operation with quick recovery.
## What does Bytevault offer?

- **Fast**: Delivers quick read and write operations for efficient data handling.
- **Thread-safe**: Ensures data integrity during concurrent access.
- **Scalable**: Designed to handle large datasets with efficient indexing.
- **Reliable**: Designed for crash-friendly operation with quick recovery.
- **Efficient**: Supports compaction and merging to optimize storage usage.
- **Modern**: Built with Java 21, leveraging the latest language features for enhanced performance.

## Limitations

- **In-Memory Key Directory**: All keys must fit in memory. While this enables fast lookups, it may limit the total number of keys that can be stored, depending on available RAM.
- **No Range Queries**: Following the Bitcask model, Bytevault does not support efficient range queries. Retrieving a range of keys requires scanning the entire key set.
- **Write Amplification**: Each write operation appends to the log file, which can lead to increased disk usage over time, especially for frequently updated keys.
- **Single Writer**: To maintain consistency, only one process can write to the database at a time, which may limit write throughput in multi-process scenarios.
- **Key Size Limit**: There may be a practical limit on key sizes to ensure efficient memory usage in the key directory.
- **Eventual Consistency**: In a distributed setup, achieving strong consistency across nodes can be challenging due to the append-only log structure.


## Potential Improvements

- [ ] Add benchmarks for various operations.
- [ ] Bloom Filters: Implement Bloom filters to reduce disk reads for non-existent keys.
- [ ] Expiration and TTL: Add support for key expiration and time-to-live (TTL) features.
- [ ] Multi-Tenancy: Develop features to support multiple isolated keyspaces within a single Bytevault instance.
- [ ] Hot/Cold Data Separation: Implement mechanisms to separate frequently and infrequently accessed data for better caching and compaction efficiency.

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[Apache 2.0](https://github.com/SanskarxRawat/bytevault/blob/master/LICENSE)
