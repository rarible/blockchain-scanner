package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainClient
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.service.FlowLogService
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventFilter
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogRecordComparator
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import kotlinx.coroutines.reactor.mono
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class FlowBlockchainScanner(
    flowClient: FlowBlockchainClient,
    subscribers: List<FlowLogEventSubscriber>,
    logFilters: List<FlowLogEventFilter>,
    blockService: BlockService,
    logService: FlowLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher,
    blockMonitor: BlockMonitor,
    logMonitor: LogMonitor
) : BlockchainScanner<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor>(
    blockchainClient = flowClient,
    subscribers = subscribers,
    logFilters = logFilters,
    blockService = blockService,
    logService = logService,
    logRecordComparator = FlowLogRecordComparator,
    properties = properties,
    logRecordEventPublisher = logRecordEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor
) {
    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }.subscribe()
    }
}
