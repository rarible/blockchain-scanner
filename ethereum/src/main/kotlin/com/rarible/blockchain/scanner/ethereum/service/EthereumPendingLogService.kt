package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.core.common.nowMillis
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class EthereumPendingLogService(
    private val ethereumLogRepository: EthereumLogRepository,
    properties: EthereumScannerProperties
) {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogService::class.java)

    private val maxPendingLogDuration = Duration.ofMillis(properties.maxPendingLogDuration)

    suspend fun getInactivePendingLogs(
        fullBlock: FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>,
        descriptor: EthereumDescriptor
    ): List<EthereumLogRecord<*>> {
        val pendingLogs = ethereumLogRepository.findPendingLogs(
            entityType = descriptor.entityType,
            collection = descriptor.collection,
            topic = descriptor.ethTopic
        ).toList()
        if (pendingLogs.isEmpty()) {
            return emptyList()
        }
        logger.info("Found {} pending logs for {}", pendingLogs.size, descriptor.id)
        val confirmedTransactions = fullBlock.logs.map { it.ethLog.transactionHash().toString() }.toSet()
        val toConfirmLogs = pendingLogs.filter { it.log.transactionHash in confirmedTransactions }

        // Logs that have received confirmation in this new block (mark them as INACTIVE).
        val confirmedLogs = toConfirmLogs.map { it.withLog(it.log.copy(status = EthereumLogStatus.INACTIVE)) }

        // Logs that have not received confirmation in max pending time (mark them as DROPPED).
        val expiredLogs = (pendingLogs - confirmedLogs)
            .filter { Duration.between(it.updatedAt, nowMillis()) > maxPendingLogDuration }
            .map { it.withLog(it.log.copy(status = EthereumLogStatus.DROPPED)) }
        logger.info(
            "Found {} confirmed logs and {} dropped logs for {}",
            confirmedLogs.size,
            expiredLogs.size,
            descriptor.id
        )
        return confirmedLogs + expiredLogs
    }
}
