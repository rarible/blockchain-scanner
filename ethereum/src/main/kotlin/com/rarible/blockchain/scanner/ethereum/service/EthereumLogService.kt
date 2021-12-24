package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.core.common.optimisticLock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EthereumLogService(
    private val ethereumLogRepository: EthereumLogRepository,
    private val ethereumPendingLogService: EthereumPendingLogService,
    private val properties: EthereumScannerProperties
) : LogService<EthereumLogRecord, EthereumDescriptor> {

    private val logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override suspend fun delete(descriptor: EthereumDescriptor, record: EthereumLogRecord): EthereumLogRecord {
        return ethereumLogRepository.delete(descriptor.collection, record)
    }

    override suspend fun delete(
        descriptor: EthereumDescriptor,
        records: List<EthereumLogRecord>
    ): List<EthereumLogRecord> = records.map { ethereumLogRepository.delete(descriptor.collection, it) }

    override suspend fun save(
        descriptor: EthereumDescriptor,
        records: List<EthereumLogRecord>
    ): List<EthereumLogRecord> {
        return records.map { record ->
            optimisticLock(properties.optimisticLockRetries) {

                val collection = descriptor.collection
                val log = record.log

                val found = ethereumLogRepository.findVisibleByKey(
                    descriptor.entityType,
                    collection,
                    log.transactionHash,
                    log.topic,
                    log.address,
                    log.index,
                    log.minorLogIndex
                )

                if (found != null) {
                    val withCorrectId = record.withIdAndVersion(found.id, found.version, found.updatedAt)
                    if (withCorrectId != found) {
                        logger.info("Saving changed LogEvent to collection '{}' : [{}]", collection, withCorrectId)
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

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: EthereumDescriptor,
        newBlock: FullBlock<*, *>
    ): List<EthereumLogRecord> {
        @Suppress("UNCHECKED_CAST")
        val fullBlock = newBlock as FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>
        return ethereumPendingLogService.getInactivePendingLogs(fullBlock, descriptor)
    }

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: EthereumDescriptor,
        revertedBlockHash: String
    ): List<EthereumLogRecord> = ethereumLogRepository.find(
        entityType = descriptor.entityType,
        collection = descriptor.collection,
        blockHash = Word.apply(revertedBlockHash),
        topic = descriptor.ethTopic
    ).toList().map { it.withLog(it.log.copy(status = EthereumLogStatus.REVERTED)) }
}
