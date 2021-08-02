package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumBlockMapper
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumLogMapper
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.service.EthereumBlockService
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.service.EthereumPendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.springframework.stereotype.Component

@Component
@FlowPreview
@ExperimentalCoroutinesApi
class EthereumBlockchainScanner(
    blockchainClient: EthereumClient,
    subscribers: List<LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog>>,
    blockMapper: EthereumBlockMapper,
    blockService: EthereumBlockService,
    logMapper: EthereumLogMapper,
    logService: EthereumLogService,
    pendingLogService: EthereumPendingLogService,
    logEventListeners: List<LogEventListener<EthereumLog>>,
    properties: BlockchainScannerProperties
) : BlockchainScanner<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumBlock, EthereumLog>(
    blockchainClient,
    subscribers,
    blockMapper,
    blockService,
    logMapper,
    logService,
    pendingLogService,
    logEventListeners,
    properties
)