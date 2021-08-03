package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.core.common.justOrEmpty
import com.rarible.core.common.toOptional
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.bson.types.ObjectId

class TestLogService(
    private val testLogRepository: TestLogRepository
) : LogService<TestLog, TestDescriptor> {

    override suspend fun delete(descriptor: TestDescriptor, log: TestLog): TestLog {
        return testLogRepository.delete(descriptor.collection, log).awaitFirst()
    }

    override suspend fun saveOrUpdate(
        descriptor: TestDescriptor,
        event: TestLog
    ): TestLog {
        val opt = testLogRepository.findByKey(
            descriptor.collection,
            event.transactionHash,
            event.blockHash!!,
            event.logIndex!!,
            event.minorLogIndex
        ).toOptional()

        return opt.flatMap {
            if (it.isPresent) {
                val found = it.get()
                val withCorrectId = event.copy(id = found.id, version = found.version)
                if (withCorrectId != found) {
                    testLogRepository.save(descriptor.collection, withCorrectId)
                } else {
                    found.justOrEmpty()
                }
            } else {
                testLogRepository.save(descriptor.collection, event)
            }
        }.awaitFirst()
    }

    override suspend fun save(descriptor: TestDescriptor, log: TestLog): TestLog {
        return testLogRepository.save(descriptor.collection, log).awaitFirst()
    }

    override fun findPendingLogs(descriptor: TestDescriptor): Flow<TestLog> {
        return testLogRepository.findPendingLogs(descriptor.collection).asFlow()
    }

    override suspend fun findLogEvent(descriptor: TestDescriptor, id: ObjectId): TestLog {
        return testLogRepository.findLogEvent(descriptor.collection, id).awaitFirst()
    }

    override fun findAndRevert(descriptor: TestDescriptor, blockHash: String): Flow<TestLog> {
        return testLogRepository.findAndRevert(descriptor.collection, blockHash, descriptor.topic).asFlow()
    }

    override fun findAndDelete(
        descriptor: TestDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<TestLog> {
        return testLogRepository.findAndDelete(descriptor.collection, blockHash, descriptor.topic, status).asFlow()
    }

    override suspend fun updateStatus(
        descriptor: TestDescriptor,
        log: TestLog,
        status: Log.Status
    ): TestLog {
        val toSave = log.copy(status = status, visible = false)
        return testLogRepository.save(descriptor.collection, toSave).awaitFirst()
    }
}