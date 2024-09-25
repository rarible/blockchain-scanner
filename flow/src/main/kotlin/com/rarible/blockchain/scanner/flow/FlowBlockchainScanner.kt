package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.util.subscribeWithRetry
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class FlowBlockchainScanner(
    manager: FlowBlockchainScannerManager
) : BlockchainScanner<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, TransactionRecord, FlowDescriptor, FlowLogRepository>(
    manager
) {

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }.subscribeWithRetry(logger)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FlowBlockchainScanner::class.java)
    }
}
