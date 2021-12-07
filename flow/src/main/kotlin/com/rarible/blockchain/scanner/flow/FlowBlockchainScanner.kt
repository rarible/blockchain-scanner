package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.client.FlowRetryableClient
import com.rarible.blockchain.scanner.flow.mapper.FlowBlockMapper
import com.rarible.blockchain.scanner.flow.mapper.FlowLogMapper
import com.rarible.blockchain.scanner.flow.model.FlowBlock
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.service.FlowBlockService
import com.rarible.blockchain.scanner.flow.service.FlowLogService
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.reactor.mono
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component


@Component
@FlowPreview
@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
class FlowBlockchainScanner(
    blockchainClient: FlowRetryableClient,
    subscribers: List<FlowLogEventSubscriber>,
    blockMapper: FlowBlockMapper,
    blockService: FlowBlockService,
    logMapper: FlowLogMapper,
    logService: FlowLogService,
    properties: BlockchainScannerProperties,
    // Autowired from core
    blockEventPublisher: BlockEventPublisher,
    blockEventConsumer: BlockEventConsumer,
    logEventPublisher: LogEventPublisher
) : BlockchainScanner<FlowBlockchainBlock, FlowBlockchainLog, FlowBlock, FlowLog, FlowLogRecord<*>, FlowDescriptor>(
    blockchainClient,
    subscribers,
    blockMapper,
    blockService,
    logMapper,
    logService,
    properties,
    blockEventPublisher,
    blockEventConsumer,
    logEventPublisher
) {

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }.subscribe()
    }
}
