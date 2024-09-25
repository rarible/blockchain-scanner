package com.rarible.blockchain.scanner.framework.model

/**
 * Describes operations with persistent storage for Log Records.
 * Each Blockchain Scanner implementation may have its own storage (Mongo/Postgres etc.)
 * and its own rules how to search/update LogRecord entities.
 * */
interface LogStorage {
    /**
     * Counts transaction log entries for the given block in the given DB table.
     * */
    suspend fun countByBlockNumber(blockNumber: Long): Long
}
