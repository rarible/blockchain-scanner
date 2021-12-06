package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.core.common.optimisticLock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EthereumLogService(
    private val ethereumLogRepository: EthereumLogRepository,
    private val pendingLogMarker: EthereumPendingLogService,
    private val properties: EthereumScannerProperties
) : LogService<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor> {

    private val logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override suspend fun delete(descriptor: EthereumDescriptor, record: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return ethereumLogRepository.delete(descriptor.collection, record)
    }

    override suspend fun save(
        descriptor: EthereumDescriptor,
        records: List<EthereumLogRecord<*>>
    ): List<EthereumLogRecord<*>> {
        return records.map { record ->
            optimisticLock(properties.optimisticLockRetries) {

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

                if (found != null) {
                    val withCorrectId = record.withIdAndVersion(found.id, found.version)
                    if (withCorrectId != found) {
                        logger.info("Saving changed LogEvent to collection '{}' : [{}]", withCorrectId, collection)
                        ethereumLogRepository.save(collection, withCorrectId)
                    } else {
                        logger.info("LogEvent wasn't changed: [{}]", withCorrectId)
                        found
                    }
                } else {
                    logger.info("Saving new LogEvent: [{}}", record)
                    ethereumLogRepository.save(collection, record)
                }
            }
        }
    }

    override suspend fun beforeHandleNewBlock(
        descriptor: EthereumDescriptor,
        blockHash: String
    ): List<EthereumLogRecord<*>> {
        return pendingLogMarker.markInactive(blockHash, descriptor)
    }

    override suspend fun findAndDelete(
        descriptor: EthereumDescriptor,
        blockHash: String,
        status: Log.Status?
    ): List<EthereumLogRecord<*>> {
        return ethereumLogRepository.findAndDelete(
            descriptor.collection,
            Word.apply(blockHash),
            descriptor.ethTopic,
            status
        ).collectList().awaitFirst()
    }
}