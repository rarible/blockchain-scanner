package com.rarible.blockchain.scanner.test

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogRecordComparator

class TestBlockchainScanner(
    blockchainClient: TestBlockchainClient,
    subscribers: List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>,
    blockService: BlockService,
    logService: TestLogService,
    properties: BlockchainScannerProperties,
    // Autowired from core
    blockEventPublisher: BlockEventPublisher,
    blockEventConsumer: BlockEventConsumer,
    logRecordEventPublisher: LogRecordEventPublisher
) : BlockchainScanner<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>(
    blockchainClient,
    subscribers,
    blockService,
    logService,
    TestLogRecordComparator,
    properties,
    blockEventPublisher,
    blockEventConsumer,
    logRecordEventPublisher
)
