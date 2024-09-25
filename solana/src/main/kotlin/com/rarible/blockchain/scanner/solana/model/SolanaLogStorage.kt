package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.LogStorage

interface SolanaLogStorage : LogStorage {

    suspend fun save(record: SolanaLogRecord): SolanaLogRecord

    suspend fun save(records: List<SolanaLogRecord>, blockHash: String): List<SolanaLogRecord> {
        return saveAll(records)
    }

    suspend fun saveAll(records: List<SolanaLogRecord>): List<SolanaLogRecord>

    suspend fun delete(record: SolanaLogRecord): SolanaLogRecord
}
