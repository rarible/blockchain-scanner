package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainClient
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.service.FlowLogService
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogRecordComparator
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import org.springframework.stereotype.Component

@Component
class FlowBlockchainScannerManager(
    flowClient: FlowBlockchainClient,
    subscribers: List<FlowLogEventSubscriber>,
    blockService: BlockService,
    logService: FlowLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher,
    blockMonitor: BlockMonitor,
    logMonitor: LogMonitor,
    reindexMonitor: ReindexMonitor,
    transactionRecordEventPublisher: TransactionRecordEventPublisher,
    logEventSubscriberExceptionResolver: FlowLogEventSubscriberExceptionResolver,
) : BlockchainScannerManager<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, TransactionRecord, FlowDescriptor>(
    blockchainClient = flowClient,
    logSubscribers = subscribers,
    blockService = blockService,
    logService = logService,
    logRecordComparator = FlowLogRecordComparator,
    properties = properties,
    logRecordEventPublisher = logRecordEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor,
    reindexMonitor = reindexMonitor,
    transactionRecordEventPublisher = transactionRecordEventPublisher,
    transactionSubscribers = emptyList(),
    logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver,
)
