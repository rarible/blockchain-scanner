package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumBlockMapper
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumBlockService
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventComparator
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
@FlowPreview
@ExperimentalCoroutinesApi
class EthereumScanner(
    ethereumClient: EthereumClient,
    subscribers: List<EthereumLogEventSubscriber>,
    blockMapper: EthereumBlockMapper,
    blockService: EthereumBlockService,
    logService: EthereumLogService,
    logEventComparator: EthereumLogEventComparator,
    properties: BlockchainScannerProperties,
    // Autowired from core
    blockEventPublisher: BlockEventPublisher,
    blockEventConsumer: BlockEventConsumer,
    logEventPublisher: LogEventPublisher
) : BlockchainScanner<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumBlock, EthereumLog, EthereumLogRecord<*>, EthereumDescriptor>(
    ethereumClient,
    subscribers,
    blockMapper,
    blockService,
    logService,
    logEventComparator,
    properties,
    blockEventPublisher,
    blockEventConsumer,
    logEventPublisher,
) {

    private val logger = LoggerFactory.getLogger(EthereumScanner::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        logger.info("Starting Ethereum Blockchain Scanner...")
        mono { (scan()) }.subscribe()
    }

}
