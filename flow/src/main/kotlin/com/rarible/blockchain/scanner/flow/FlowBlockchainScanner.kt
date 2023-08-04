package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import reactor.util.retry.Retry

@Component
class FlowBlockchainScanner(
    manager: FlowBlockchainScannerManager
) : BlockchainScanner<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, TransactionRecord, FlowDescriptor>(
    manager
) {

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }
            .doOnError {
                logger.warn("Scanner stopped with error. Will restart", it)
            }
            .retryWhen(Retry.indefinitely())
            .subscribe({}, { logger.error("Flow blockchain scanner stopped.", it) })
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FlowBlockchainScanner::class.java)
    }
}
