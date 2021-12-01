package com.rarible.blockchain.scanner.test

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.mapper.TestLogMapper
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.service.TestBlockService
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.service.TestPendingLogService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview

@ExperimentalCoroutinesApi
@FlowPreview
class TestBlockchainScanner(
    blockchainClient: TestBlockchainClient,
    subscribers: List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor>>,
    blockMapper: TestBlockMapper,
    blockService: TestBlockService,
    logMapper: TestLogMapper,
    logService: TestLogService,
    pendingLogService: TestPendingLogService,
    logEventListeners: List<LogEventListener<TestLog, TestLogRecord<*>>>,
    properties: BlockchainScannerProperties,
    // Autowired from core
    blockEventPublisher: BlockEventPublisher,
    blockEventConsumer: BlockEventConsumer
) : BlockchainScanner<TestBlockchainBlock, TestBlockchainLog, TestBlock, TestLog, TestLogRecord<*>, TestDescriptor>(
    blockchainClient,
    subscribers,
    blockMapper,
    blockService,
    logMapper,
    logService,
    pendingLogService,
    logEventListeners,
    properties,
    blockEventPublisher,
    blockEventConsumer
)
