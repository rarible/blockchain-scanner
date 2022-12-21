package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventFilter
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogRecordComparator
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import org.springframework.stereotype.Component

@Component
class LogHandlerFactory(
    ethereumClient: EthereumBlockchainClient,
    blockchainScannerProperties: BlockchainScannerProperties,
    private val logService: EthereumLogService,
    private val logFilters: List<EthereumLogEventFilter>,
    private val logMonitor: LogMonitor
) {
    private val retryableClient = RetryableBlockchainClient(
        original = ethereumClient,
        retryPolicy = blockchainScannerProperties.retryPolicy.client
    )

    fun create(
        groupId: String,
        subscribers: List<EthereumLogEventSubscriber>,
        logService: LogService<EthereumLogRecord, EthereumDescriptor>,
        logRecordEventPublisher: LogRecordEventPublisher,
    ) = LogHandler(
            groupId = groupId,
            blockchainClient = retryableClient,
            subscribers = subscribers,
            logFilters = logFilters,
            logService = logService,
            logRecordComparator = EthereumLogRecordComparator,
            logRecordEventPublisher = logRecordEventPublisher,
            logMonitor = logMonitor
        )

    fun create(
        groupId: String,
        subscribers: List<EthereumLogEventSubscriber>,
        logRecordEventPublisher: LogRecordEventPublisher,
    ) = create(groupId, subscribers, logService, logRecordEventPublisher)
}