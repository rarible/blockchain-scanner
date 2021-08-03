package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.core.common.justOrEmpty
import com.rarible.core.common.toOptional
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst

class TestLogService(
    private val testLogRepository: TestLogRepository
) : LogService<TestLog, TestLogRecord, TestDescriptor> {

    override suspend fun delete(descriptor: TestDescriptor, record: TestLogRecord): TestLogRecord {
        return testLogRepository.delete(descriptor.collection, record).awaitFirst()
    }

    override suspend fun saveOrUpdate(
        descriptor: TestDescriptor,
        record: TestLogRecord
    ): TestLogRecord {
        val log = record.log!!
        val opt = testLogRepository.findByKey(
            descriptor.collection,
            log.transactionHash,
            log.blockHash!!,
            log.logIndex!!,
            log.minorLogIndex
        ).toOptional()

        return opt.flatMap {
            if (it.isPresent) {
                val found = it.get()
                record.id = found.id
                record.version = found.version
                if (record != found) {
                    testLogRepository.save(descriptor.collection, record)
                } else {
                    found.justOrEmpty()
                }
            } else {
                testLogRepository.save(descriptor.collection, record)
            }
        }.awaitFirst()
    }

    override suspend fun save(descriptor: TestDescriptor, record: TestLogRecord): TestLogRecord {
        return testLogRepository.save(descriptor.collection, record).awaitFirst()
    }

    override fun findPendingLogs(descriptor: TestDescriptor): Flow<TestLogRecord> {
        return testLogRepository.findPendingLogs(descriptor.collection).asFlow()
    }

    override fun findAndRevert(descriptor: TestDescriptor, blockHash: String): Flow<TestLogRecord> {
        return testLogRepository.findAndRevert(descriptor.collection, blockHash, descriptor.topic).asFlow()
    }

    override fun findAndDelete(
        descriptor: TestDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<TestLogRecord> {
        return testLogRepository.findAndDelete(descriptor.collection, blockHash, descriptor.topic, status).asFlow()
    }

    override suspend fun updateStatus(
        descriptor: TestDescriptor,
        record: TestLogRecord,
        status: Log.Status
    ): TestLogRecord {
        record.log = record.log!!.copy(status = status, visible = false)
        return testLogRepository.save(descriptor.collection, record).awaitFirst()
    }
}