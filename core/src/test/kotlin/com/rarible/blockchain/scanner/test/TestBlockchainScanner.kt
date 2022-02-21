package com.rarible.blockchain.scanner.test

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.blockchain.scanner.test.subscriber.TestLogRecordComparator

class TestBlockchainScanner(
    blockchainClient: TestBlockchainClient,
    subscribers: List<TestLogEventSubscriber>,
    blockService: BlockService,
    logService: TestLogService,
    properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher,
    monitor: BlockMonitor
) : BlockchainScanner<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>(
    blockchainClient,
    subscribers,
    blockService,
    logService,
    TestLogRecordComparator,
    properties,
    logRecordEventPublisher,
    monitor
)
