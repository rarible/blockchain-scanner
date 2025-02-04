package com.rarible.blockchain.scanner.hedera.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage
import org.springframework.stereotype.Component

@Component
class HederaLogService : LogService<HederaLogRecord, HederaDescriptor, HederaLogStorage> {

    override suspend fun save(
        descriptor: HederaDescriptor,
        records: List<HederaLogRecord>,
        blockHash: String,
    ): List<HederaLogRecord> = descriptor.storage.saveAll(records)

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: HederaDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<HederaLogRecord> = emptyList()

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: HederaDescriptor,
        revertedBlockHash: String
    ): List<HederaLogRecord> = emptyList()
}
