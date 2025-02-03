package com.rarible.blockchain.scanner.hedera

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainClient
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainLog
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage
import com.rarible.blockchain.scanner.hedera.service.HederaLogService
import com.rarible.blockchain.scanner.hedera.subscriber.HederaLogEventSubscriber
import com.rarible.blockchain.scanner.hedera.subscriber.HederaLogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.hedera.subscriber.HederaLogRecordComparator
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import org.springframework.stereotype.Component

@Component
class HederaBlockchainScannerManager(
    blockchainClient: HederaBlockchainClient,
    subscribers: List<HederaLogEventSubscriber>,
    blockService: BlockService,
    logService: HederaLogService,
    properties: BlockchainScannerProperties,
    logEventPublisher: LogRecordEventPublisher,
    blockMonitor: BlockMonitor,
    logMonitor: LogMonitor,
    reindexMonitor: ReindexMonitor,
    transactionRecordEventPublisher: TransactionRecordEventPublisher,
    logEventSubscriberExceptionResolver: HederaLogEventSubscriberExceptionResolver,
) : BlockchainScannerManager<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, TransactionRecord, HederaDescriptor, HederaLogStorage>(
    blockchainClient = blockchainClient,
    logSubscribers = subscribers,
    blockService = blockService,
    logService = logService,
    logRecordComparator = HederaLogRecordComparator,
    properties = properties,
    logRecordEventPublisher = logEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor,
    reindexMonitor = reindexMonitor,
    transactionRecordEventPublisher = transactionRecordEventPublisher,
    transactionSubscribers = emptyList(),
    logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver,
)
