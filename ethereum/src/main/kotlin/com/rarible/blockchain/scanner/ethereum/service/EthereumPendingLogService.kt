package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.core.common.nowMillis
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class EthereumPendingLogService(
    private val ethereumLogRepository: EthereumLogRepository,
    private val logEventPublisher: LogEventPublisher,
    subscribers: List<EthereumLogEventSubscriber>
) {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogService::class.java)
    private val descriptors = subscribers.map { it.getDescriptor() }

    suspend fun dropInactivePendingLogs(
        fullBlock: FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>,
        descriptor: EthereumDescriptor
    ) {
        val pendingLogs = findPendingLogs(descriptor).toList()
        if (pendingLogs.isEmpty()) {
            return
        }
        val confirmedTransactions = fullBlock.logs.map { it.ethLog.transactionHash().toString() }.toSet()
        val toConfirmLogs = pendingLogs.filter { it.log.transactionHash in confirmedTransactions }
        logger.info(
            "Confirming {} pending logs from block {}:{}",
            toConfirmLogs.size,
            fullBlock.block.number,
            fullBlock.block.hash
        )
        val confirmedLogs = toConfirmLogs.map { it.withLog(it.log.copy(status = Log.Status.INACTIVE)) }
        logEventPublisher.publishDismissedLogs(descriptor, Source.PENDING, confirmedLogs)
        toConfirmLogs.forEach { ethereumLogRepository.delete(descriptor.collection, it) }
        logger.info(
            "Confirmed {} pending logs from block {}:{}",
            confirmedLogs.size,
            fullBlock.block.number,
            fullBlock.block.hash
        )
    }

    /**
     * Drop pending logs that haven't had confirmation in a given time ([maxPendingLogDuration]).
     */
    suspend fun dropExpiredPendingLogs(maxPendingLogDuration: Duration) {
        for (descriptor in descriptors) {
            val expiredLogs = findPendingLogs(descriptor)
                .filter { Duration.between(it.updatedAt, nowMillis()) > maxPendingLogDuration }
                .toList()
            if (expiredLogs.isNotEmpty()) {
                logger.info("Dropping {} expired pending logs for descriptor {}", expiredLogs.size, descriptor.id)
                val droppedLogs = expiredLogs.map { it.withLog(it.log.copy(status = Log.Status.DROPPED)) }
                logEventPublisher.publishDismissedLogs(descriptor, Source.PENDING, droppedLogs)
                droppedLogs.forEach { ethereumLogRepository.delete(descriptor.collection, it) }
                logger.info("Dropped {} expired pending logs for descriptor {}", droppedLogs.size, descriptor.id)
            } else {
                logger.info("No pending logs to drop for descriptor {}", descriptor.id)
            }
        }
    }

    private fun findPendingLogs(descriptor: EthereumDescriptor): Flow<EthereumLogRecord<*>> =
        ethereumLogRepository.findPendingLogs(descriptor.entityType, descriptor.collection, descriptor.ethTopic)
}
