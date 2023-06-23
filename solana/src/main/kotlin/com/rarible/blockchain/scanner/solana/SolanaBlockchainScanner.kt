package com.rarible.blockchain.scanner.solana

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class SolanaBlockchainScanner(
    manager: SolanaBlockchainScannerManager
) : BlockchainScanner<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaLogRecord, TransactionRecord, SolanaDescriptor>(
    manager = manager
) {

    private val logger = LoggerFactory.getLogger(SolanaBlockchainScanner::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { scan() }.subscribe(
            { logger.info("Solana blockchain scanner finished") },
            { logger.error("Solana blockchain scanner stopped.", it) }
        )
    }
}
