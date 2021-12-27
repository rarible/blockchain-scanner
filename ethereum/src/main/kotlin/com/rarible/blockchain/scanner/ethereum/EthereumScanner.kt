package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogRecordComparator
import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class EthereumScanner(
    ethereumClient: EthereumBlockchainClient,
    subscribers: List<EthereumLogEventSubscriber>,
    blockService: BlockService,
    logService: EthereumLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher
) : BlockchainScanner<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>(
    ethereumClient,
    subscribers,
    blockService,
    logService,
    EthereumLogRecordComparator,
    properties,
    logRecordEventPublisher,
) {

    private val logger = LoggerFactory.getLogger(EthereumScanner::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        logger.info("Starting Ethereum Blockchain Scanner...")
        mono { (scan()) }.subscribe()
    }

}
