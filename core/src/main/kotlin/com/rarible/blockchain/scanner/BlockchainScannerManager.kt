package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.framework.subscriber.TransactionEventSubscriber
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import com.rarible.blockchain.scanner.reindex.BlockChecker
import com.rarible.blockchain.scanner.reindex.BlockHandlerFactory
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.BlockScanPlanner
import com.rarible.blockchain.scanner.reindex.LogHandlerFactory

// TODO not really a good way to construct generic components, but don't see other way to do it
open class BlockchainScannerManager<
    BB : BlockchainBlock,
    BL : BlockchainLog,
    R : LogRecord,
    TR : TransactionRecord,
    D : Descriptor<S>,
    S : LogStorage
    >(
    blockchainClient: BlockchainClient<BB, BL, D>,
    val properties: BlockchainScannerProperties,
    val logSubscribers: List<LogEventSubscriber<BB, BL, R, D, S>>,
    val blockService: BlockService,
    val logService: LogService<R, D, S>,
    val logRecordComparator: LogRecordComparator<R>,
    val logRecordEventPublisher: LogRecordEventPublisher,
    val blockMonitor: BlockMonitor,
    val logMonitor: LogMonitor,
    val reindexMonitor: ReindexMonitor,
    val transactionSubscribers: List<TransactionEventSubscriber<BB, TR>>,
    val transactionRecordEventPublisher: TransactionRecordEventPublisher,
    val logEventSubscriberExceptionResolver: LogEventSubscriberExceptionResolver
) {

    constructor(blockchainClient: BlockchainClient<BB, BL, D>, manager: BlockchainScannerManager<BB, BL, R, TR, D, S>)
        : this(
        blockchainClient = blockchainClient,
        properties = manager.properties,
        logSubscribers = manager.logSubscribers,
        blockService = manager.blockService,
        logService = manager.logService,
        logRecordComparator = manager.logRecordComparator,
        logRecordEventPublisher = manager.logRecordEventPublisher,
        blockMonitor = manager.blockMonitor,
        logMonitor = manager.logMonitor,
        reindexMonitor = manager.reindexMonitor,
        transactionSubscribers = manager.transactionSubscribers,
        transactionRecordEventPublisher = manager.transactionRecordEventPublisher,
        logEventSubscriberExceptionResolver = manager.logEventSubscriberExceptionResolver,
    )

    val retryableClient = RetryableBlockchainClient(
        original = blockchainClient,
        retryPolicy = properties.retryPolicy.client
    )

    val logHandlerFactory = LogHandlerFactory(
        blockchainClient = retryableClient,
        logService = logService,
        logRecordComparator = logRecordComparator,
        logMonitor = logMonitor,
        logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver
    )

    val blockHandlerFactory = BlockHandlerFactory<BB, BL, R, D, S>(
        blockchainClient = retryableClient,
        blockService = blockService,
        blockMonitor = blockMonitor,
        properties = properties
    )

    val blockScanPlanner = BlockScanPlanner(
        blockService = blockService,
        blockchainClient = retryableClient,
        properties = properties
    )

    val blockReindexer = BlockReindexer(
        subscribers = logSubscribers,
        blockHandlerFactory = blockHandlerFactory,
        logHandlerFactory = logHandlerFactory
    )

    val blockChecker = BlockChecker(
        blockchainClient = retryableClient,
        blockService = blockService,
        reindexer = blockReindexer,
        planner = blockScanPlanner,
        properties = properties
    )
}
