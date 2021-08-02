package com.rarible.blockchain.scanner.test

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.mapper.TestLogMapper
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.service.TestBlockService
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.service.TestPendingLogService

class TestScanner(
    blockchainClient: TestBlockchainClient,
    subscribers: List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog>>,
    blockMapper: TestBlockMapper,
    blockService: TestBlockService,
    logMapper: TestLogMapper,
    logService: TestLogService,
    logEventListeners: List<LogEventListener<TestLog>>,
    pendingLogService: TestPendingLogService,
    logEventPostProcessors: List<LogEventPostProcessor<TestLog>>,
    properties: BlockchainScannerProperties
) : BlockchainScanner<TestBlockchainBlock, TestBlockchainLog, TestBlock, TestLog>(
    blockchainClient,
    subscribers,
    blockMapper,
    blockService,
    logMapper,
    logService,
    logEventListeners,
    pendingLogService,
    logEventPostProcessors,
    properties
)
