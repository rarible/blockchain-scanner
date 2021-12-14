package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import kotlinx.coroutines.reactive.awaitFirst

class TestLogService(
    private val testLogRepository: TestLogRepository
) : LogService<TestLog, TestLogRecord<*>, TestDescriptor> {

    override suspend fun delete(descriptor: TestDescriptor, record: TestLogRecord<*>): TestLogRecord<*> {
        return testLogRepository.delete(descriptor.collection, record).awaitFirst()
    }

    override suspend fun save(
        descriptor: TestDescriptor,
        records: List<TestLogRecord<*>>
    ): List<TestLogRecord<*>> {
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

    override suspend fun findAndDelete(
        descriptor: TestDescriptor,
        blockHash: String,
        status: Log.Status?
    ): List<TestLogRecord<*>> {
        return testLogRepository.findAndDelete(
            descriptor.entityType,
            descriptor.collection,
            blockHash,
            descriptor.id,
            status
        ).collectList().awaitFirst()
    }

}
