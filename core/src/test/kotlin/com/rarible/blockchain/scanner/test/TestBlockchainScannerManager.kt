package com.rarible.blockchain.scanner.test

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.model.TestTransactionRecord
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventFilter
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.blockchain.scanner.test.subscriber.TestLogRecordComparator
import com.rarible.blockchain.scanner.test.subscriber.TestTransactionEventSubscriber

class TestBlockchainScannerManager(
    blockchainClient: TestBlockchainClient,
    subscribers: List<TestLogEventSubscriber>,
    logFilters: List<TestLogEventFilter>,
    blockService: BlockService,
    logService: TestLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher,
    blockMonitor: BlockMonitor,
    logMonitor: LogMonitor,
    reindexMonitor: ReindexMonitor,
    transactionRecordEventPublisher: TransactionRecordEventPublisher,
    transactionSubscribers: List<TestTransactionEventSubscriber>,
) : BlockchainScannerManager<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestTransactionRecord, TestDescriptor>(
    blockchainClient = blockchainClient,
    logSubscribers = subscribers,
    logFilters = logFilters,
    blockService = blockService,
    logService = logService,
    logRecordComparator = TestLogRecordComparator,
    properties = properties,
    logRecordEventPublisher = logRecordEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor,
    reindexMonitor = reindexMonitor,
    transactionRecordEventPublisher = transactionRecordEventPublisher,
    transactionSubscribers = transactionSubscribers,
)
