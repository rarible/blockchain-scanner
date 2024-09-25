package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.model.revert
import com.rarible.blockchain.scanner.test.repository.TestLogStorage
import kotlinx.coroutines.reactive.awaitFirst

class TestLogService : LogService<TestLogRecord, TestDescriptor, TestLogStorage> {

    override suspend fun delete(descriptor: TestDescriptor, record: TestLogRecord): TestLogRecord =
        descriptor.storage.delete(record)

    override suspend fun save(
        descriptor: TestDescriptor,
        records: List<TestLogRecord>,
        blockHash: String,
    ): List<TestLogRecord> {
        return records.map { record ->
            val log = record.log
            val opt = descriptor.storage.findByKey(
                log.transactionHash,
                log.blockHash!!,
                log.logIndex!!,
                log.minorLogIndex
            )

            if (opt != null) {
                val withCorrectId = record.withIdAndVersion(opt.id, opt.version)
                if (withCorrectId != opt) {
                    descriptor.storage.save(withCorrectId)
                } else {
                    opt
                }
            } else {
                descriptor.storage.save(record)
            }
        }
    }

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: TestDescriptor,
        revertedBlockHash: String
    ): List<TestLogRecord> =
        descriptor.storage.find(revertedBlockHash, descriptor.id)
            .collectList()
            .awaitFirst()
            .map { it.revert() }

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: TestDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<TestLogRecord> = emptyList()
}
