package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
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
) : LogService<TestLog> {

    override suspend fun delete(collection: String, log: TestLog): TestLog {
        return testLogRepository.delete(collection, log).awaitFirst()
    }

    override suspend fun saveOrUpdate(
        collection: String,
        event: TestLog
    ): TestLog {
        val opt = testLogRepository.findByKey(
            collection,
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
                    testLogRepository.save(collection, withCorrectId)
                } else {
                    found.justOrEmpty()
                }
            } else {
                testLogRepository.save(collection, event)
            }
        }.awaitFirst()
    }

    override suspend fun save(collection: String, log: TestLog): TestLog {
        return testLogRepository.save(collection, log).awaitFirst()
    }

    override fun findPendingLogs(collection: String): Flow<TestLog> {
        return testLogRepository.findPendingLogs(collection).asFlow()
    }

    override suspend fun findLogEvent(collection: String, id: ObjectId): TestLog {
        return testLogRepository.findLogEvent(collection, id).awaitFirst()
    }

    override fun findAndRevert(collection: String, blockHash: String, topic: String): Flow<TestLog> {
        return testLogRepository.findAndRevert(collection, blockHash, topic).asFlow()
    }

    override fun findAndDelete(
        collection: String,
        blockHash: String,
        topic: String,
        status: Log.Status?
    ): Flow<TestLog> {
        return testLogRepository.findAndDelete(collection, blockHash, topic, status).asFlow()
    }

    override suspend fun updateStatus(
        collection: String,
        log: TestLog,
        status: Log.Status
    ): TestLog {
        val toSave = log.copy(status = status, visible = false)
        return testLogRepository.save(collection, toSave).awaitFirst()
    }
}