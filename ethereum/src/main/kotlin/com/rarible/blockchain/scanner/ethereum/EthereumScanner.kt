package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import reactor.util.retry.Retry

@Component
class EthereumScanner(
    manager: EthereumScannerManager
) : BlockchainScanner<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, TransactionRecord, EthereumDescriptor>(
    manager
) {

    private val logger = LoggerFactory.getLogger(EthereumScanner::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        logger.info("Starting Ethereum Blockchain Scanner...")
        mono { (scan()) }
            .doOnError {
                logger.warn("Scanner stopped with error. Will restart", it)
            }
            .retryWhen(Retry.indefinitely())
            .subscribe({}, { logger.error("Ethereum blockchain scanner stopped.", it) })
    }
}
