package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.model.revert
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import kotlinx.coroutines.reactive.awaitFirst

class TestLogService(
    private val testLogRepository: TestLogRepository
) : LogService<TestLogRecord, TestDescriptor> {

    override suspend fun delete(descriptor: TestDescriptor, record: TestLogRecord): TestLogRecord =
        testLogRepository.delete(descriptor.collection, record).awaitFirst()

    override suspend fun delete(
        descriptor: TestDescriptor,
        records: List<TestLogRecord>
    ): List<TestLogRecord> = records.map { delete(descriptor, it) }

    override suspend fun save(
        descriptor: TestDescriptor,
        records: List<TestLogRecord>,
        blockHash: String,
    ): List<TestLogRecord> {
        return records.map { record ->
            val log = record.log
            val opt = testLogRepository.findByKey(
                descriptor.entityType,
                descriptor.collection,
                log.transactionHash,
                log.blockHash!!,
                log.logIndex!!,
                log.minorLogIndex
            )

            if (opt != null) {
                val withCorrectId = record.withIdAndVersion(opt.id, opt.version)
                if (withCorrectId != opt) {
                    testLogRepository.save(descriptor.collection, withCorrectId)
                } else {
                    opt
                }
            } else {
                testLogRepository.save(descriptor.collection, record)
            }
        }
    }

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: TestDescriptor,
        revertedBlockHash: String
    ): List<TestLogRecord> =
        testLogRepository.find(
            descriptor.entityType,
            descriptor.collection,
            revertedBlockHash,
            descriptor.id
        ).collectList().awaitFirst().map { it.revert() }

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: TestDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<TestLogRecord> = emptyList()
}
