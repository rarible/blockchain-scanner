package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service

@ExperimentalCoroutinesApi
@FlowPreview
@Service
class FlowLogService(
    private val logRepository: FlowLogRepository
) : LogService<FlowLog, FlowLogRecord<*>, FlowDescriptor> {

    private val logger = LoggerFactory.getLogger(FlowLogService::class.java)

    override suspend fun delete(descriptor: FlowDescriptor, record: FlowLogRecord<*>): FlowLogRecord<*> =
        logRepository.delete(descriptor.collection, record)


    override suspend fun save(descriptor: FlowDescriptor, records: List<FlowLogRecord<*>>): List<FlowLogRecord<*>> {
        return if (descriptor.optLockOnSave) {
            logRepository.saveAll(descriptor.collection, records).toList()
        } else {
            try {
                logRepository.insert(descriptor.collection, records).toList()
            } catch (e: DuplicateKeyException) {
                logger.warn("Unable to insert logs without optimistic lock. Trying to saving logs with it.")
                logRepository.saveAll(descriptor.collection, records).toList()
            }
        }
    }

    override fun findPendingLogs(descriptor: FlowDescriptor): Flow<FlowLogRecord<*>> = emptyFlow()

    override fun findAndRevert(descriptor: FlowDescriptor, blockHash: String): Flow<FlowLogRecord<*>> = emptyFlow()

    override fun findAndDelete(
        descriptor: FlowDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<FlowLogRecord<*>> {
        return emptyFlow()
    }

    override suspend fun updateStatus(
        descriptor: FlowDescriptor,
        record: FlowLogRecord<*>,
        status: Log.Status
    ): FlowLogRecord<*> =
        logRepository.save(descriptor.collection, record.withLog(record.log.copy(status = status)))
}
