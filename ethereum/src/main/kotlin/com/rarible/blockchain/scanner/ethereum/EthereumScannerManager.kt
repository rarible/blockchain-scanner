package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventFilter
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogRecordComparator
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import org.springframework.stereotype.Component

@Component
class EthereumScannerManager(
    ethereumClient: EthereumBlockchainClient,
    subscribers: List<EthereumLogEventSubscriber>,
    logFilters: List<EthereumLogEventFilter>,
    blockService: BlockService,
    logService: EthereumLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher,
    blockMonitor: BlockMonitor,
    logMonitor: LogMonitor,
    reindexMonitor: ReindexMonitor
) : BlockchainScannerManager<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>(
    blockchainClient = ethereumClient,
    subscribers = subscribers,
    logFilters = logFilters,
    blockService = blockService,
    logService = logService,
    logRecordComparator = EthereumLogRecordComparator,
    properties = properties,
    logRecordEventPublisher = logRecordEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor,
    reindexMonitor = reindexMonitor
)