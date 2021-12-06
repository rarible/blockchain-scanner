package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.core.common.justOrEmpty
import com.rarible.core.common.toOptional
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
            val log = record.log!!
            val opt = testLogRepository.findByKey(
                descriptor.collection,
                log.transactionHash,
                log.blockHash!!,
                log.logIndex!!,
                log.minorLogIndex
            ).toOptional()

            opt.flatMap {
                if (it.isPresent) {
                    val found = it.get()
                    val withCorrectId = record.withIdAndVersion(found.id, found.version)
                    if (withCorrectId != found) {
                        testLogRepository.save(descriptor.collection, withCorrectId)
                    } else {
                        found.justOrEmpty()
                    }
                } else {
                    testLogRepository.save(descriptor.collection, record)
                }
            }.awaitFirst()
        }
    }

    override suspend fun findAndDelete(
        descriptor: TestDescriptor,
        blockHash: String,
        status: Log.Status?
    ): List<TestLogRecord<*>> {
        return testLogRepository.findAndDelete(descriptor.collection, blockHash, descriptor.id, status)
            .collectList().awaitFirst()
    }

    override suspend fun beforeHandleNewBlock(descriptor: TestDescriptor, blockHash: String): List<TestLogRecord<*>> {
        return emptyList()
    }
}