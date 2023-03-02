package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.core.common.optimisticLock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Component

@Component
class EthereumLogService(
    private val ethereumLogRepository: EthereumLogRepository,
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
        logger.info("Saving records: {} for {}", records.size, descriptor.ethTopic)
        return records.map { record ->
            optimisticLock(properties.optimisticLockRetries) {
                save(descriptor, record)
            }
        }
    }

    private suspend fun save(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord
    ): EthereumLogRecord {
        val collection = descriptor.collection
        val log = record.log

        var found = ethereumLogRepository.findVisibleByKey(
            descriptor.entityType,
            collection,
            log.transactionHash,
            log.topic,
            log.address,
            log.index,
            log.minorLogIndex
        )

        if (found == null) {
            try {
                val result = ethereumLogRepository.save(collection, record)
                logger.info("Saved new LogEvent: [{}]", record)
                return result
            } catch (e: DuplicateKeyException) {
                found = getLegacyDuplicate(descriptor, record) ?: throw e
            }
        }

        if (found == null) {
            // Previous "if" branches can't allow null here, but compiler doesn't think so
            throw NullPointerException("Can't happen")
        }

        val withCorrectId = record.withIdAndVersion(found.id, found.version, found.updatedAt)
        return if (withCorrectId != found) {
            logger.info("Saving changed LogEvent to collection '{}' : [{}]", collection, withCorrectId)
            ethereumLogRepository.save(collection, withCorrectId)
        } else {
            logger.info("LogEvent wasn't changed: [{}]", withCorrectId)
            found
        }
    }

    private suspend fun getLegacyDuplicate(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord
    ): EthereumLogRecord? {
        val collection = descriptor.collection
        val log = record.log
        // Workaround for legacy logs we meet during reindexing
        val duplicate = ethereumLogRepository.findLegacyRecord(
            descriptor.entityType,
            collection,
            log.transactionHash,
            log.blockHash!!,
            log.logIndex!!,
            log.minorLogIndex
        ) ?: return null // Should not be null

        return if (duplicate.log.status == EthereumLogStatus.REVERTED) {
            logger.warn("Found legacy duplicate: [{}], new record: [{}]", duplicate, record)
            duplicate
        } else {
            logger.error(
                "Found duplicated legacy record with status != REVERTED: [{}], new record: [{}]",
                duplicate, record
            )
            null
        }
    }

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: EthereumDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<EthereumLogRecord> {
        return emptyList()
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
