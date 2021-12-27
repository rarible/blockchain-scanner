package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainClient
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.service.FlowLogService
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogRecordComparator
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import kotlinx.coroutines.reactor.mono
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component


@Component
class FlowBlockchainScanner(
    flowBlockchainClient: FlowBlockchainClient,
    subscribers: List<FlowLogEventSubscriber>,
    blockService: BlockService,
    logService: FlowLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher
) : BlockchainScanner<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor>(
    flowBlockchainClient,
    subscribers,
    blockService,
    logService,
    FlowLogRecordComparator,
    properties,
    logRecordEventPublisher
) {

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }.subscribe()
    }
}
