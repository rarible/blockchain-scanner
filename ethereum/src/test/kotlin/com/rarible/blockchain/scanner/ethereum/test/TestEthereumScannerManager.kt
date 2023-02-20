package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventFilter
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import io.mockk.mockk

class TestEthereumScannerManager(
    ethereumClient: EthereumBlockchainClient = mockk(),
    subscribers: List<EthereumLogEventSubscriber> = mockk(),
    logFilters: List<EthereumLogEventFilter> = mockk(),
    blockService: BlockService = mockk(),
    logService: EthereumLogService = mockk(),
    properties: BlockchainScannerProperties = mockk(),
    logRecordEventPublisher: LogRecordEventPublisher = mockk(),
    blockMonitor: BlockMonitor = mockk(),
    logMonitor: LogMonitor = mockk(),
    reindexMonitor: ReindexMonitor = mockk()
) : EthereumScannerManager(
    ethereumClient = ethereumClient,
    subscribers = subscribers,
    logFilters = logFilters,
    blockService = blockService,
    logService = logService,
    properties = properties,
    logRecordEventPublisher = logRecordEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor,
    reindexMonitor = reindexMonitor
)