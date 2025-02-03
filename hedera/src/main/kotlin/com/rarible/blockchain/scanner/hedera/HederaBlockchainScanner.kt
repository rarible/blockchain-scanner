package com.rarible.blockchain.scanner.hedera

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainLog
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class HederaBlockchainScanner(
    manager: HederaBlockchainScannerManager
) : BlockchainScanner<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, TransactionRecord, HederaDescriptor, HederaLogStorage>(
    manager = manager
) {
    private val logger = LoggerFactory.getLogger(HederaBlockchainScanner::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { scan() }.subscribe(
            { logger.info("Hedera blockchain scanner finished") },
            { logger.error("Hedera blockchain scanner stopped.", it) }
        )
    }
}
