package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EthereumLogService(
    private val ethereumLogRepository: EthereumLogRepository
) : LogService<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override suspend fun delete(descriptor: EthereumDescriptor, record: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return ethereumLogRepository.delete(descriptor.collection, record)
    }

    override suspend fun save(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord<*>
    ): EthereumLogRecord<*> {

        val collection = descriptor.collection
        val log = record.log!!

        val found = ethereumLogRepository.findVisibleByKey(
            collection,
            log.transactionHash,
            log.topic,
            log.index,
            log.minorLogIndex
        ) ?: ethereumLogRepository.findByKey(
            collection,
            log.transactionHash,
            log.blockHash!!,
            log.logIndex!!,
            log.minorLogIndex
        )

        return if (found != null) {
            val withCorrectId = record.withIdAndVersion(found.id, found.version)
            if (withCorrectId != found) {
                logger.info("Saving changed LogEvent to collection '{}' : [{}]", withCorrectId, collection)
                ethereumLogRepository.save(collection, withCorrectId)
            } else {
                logger.info("LogEvent didn't change: [{}]", withCorrectId)
                found
            }
        } else {
            logger.info("Saving new LogEvent: [{}}", record)
            ethereumLogRepository.save(collection, record)
        }
    }

    override fun findPendingLogs(descriptor: EthereumDescriptor): Flow<EthereumLogRecord<*>> {
        return ethereumLogRepository.findPendingLogs(descriptor.collection, descriptor.topic).asFlow()
    }

    override fun findAndRevert(descriptor: EthereumDescriptor, blockHash: String): Flow<EthereumLogRecord<*>> {
        return ethereumLogRepository.findAndRevert(
            descriptor.collection,
            Word.apply(blockHash),
            descriptor.topic
        ).asFlow()
    }

    override fun findAndDelete(
        descriptor: EthereumDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<EthereumLogRecord<*>> {
        return ethereumLogRepository.findAndDelete(
            descriptor.collection,
            Word.apply(blockHash),
            descriptor.topic,
            status
        ).asFlow()
    }

    override suspend fun updateStatus(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord<*>,
        status: Log.Status
    ): EthereumLogRecord<*> {
        val copy = record.withLog(record.log!!.copy(status = status, visible = false))
        return ethereumLogRepository.save(descriptor.collection, copy)
    }
}