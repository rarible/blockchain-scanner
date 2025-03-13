package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.util.subscribeWithRetry
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class EthereumScanner(
    @Qualifier("ethereumScannerManager") manager: EthereumScannerManager
) : BlockchainScanner<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, TransactionRecord, EthereumDescriptor, EthereumLogRepository>(
    manager
) {

    private val logger = LoggerFactory.getLogger(EthereumScanner::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        logger.info("Starting Ethereum Blockchain Scanner...")
        mono { (scan()) }.subscribeWithRetry(logger)
    }
}
