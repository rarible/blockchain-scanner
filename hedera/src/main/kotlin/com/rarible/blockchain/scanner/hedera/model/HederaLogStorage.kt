package com.rarible.blockchain.scanner.hedera.model

import com.rarible.blockchain.scanner.framework.model.LogStorage

interface HederaLogStorage : LogStorage {

    suspend fun save(record: HederaLogRecord): HederaLogRecord

    suspend fun save(records: List<HederaLogRecord>, blockHash: String): List<HederaLogRecord> {
        return saveAll(records)
    }

    suspend fun saveAll(records: List<HederaLogRecord>): List<HederaLogRecord>

    suspend fun delete(record: HederaLogRecord): HederaLogRecord
}
