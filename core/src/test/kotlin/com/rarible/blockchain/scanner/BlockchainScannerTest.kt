package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockPublishProperties
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.JobProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.TestBlockchainScannerProperties
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.service.TestBlockService
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.core.daemon.DaemonWorkerProperties
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Duration

class BlockchainScannerTest {
    private val testLogService = mockk<TestLogService>()
    private val testBlockService = mockk<TestBlockService>()
    private val blockchainClient = mockk<TestBlockchainClient>()
    private val blockEventPublisher = mockk<BlockEventPublisher>()
    private val blockEventConsumer = mockk<BlockEventConsumer>()
    private val logRecordEventPublisher = mockk<LogRecordEventPublisher>()

    @Test
    fun `should not run block publish`() = runBlocking<Unit> {
        val properties = TestBlockchainScannerProperties(
            retryPolicy = RetryPolicyProperties(
                scan = ScanRetryPolicyProperties(
                    reconnectDelay = Duration.ofMillis(1),
                    reconnectAttempts = 0
                ),
                client = ClientRetryPolicyProperties(
                    delay = Duration.ofMillis(1),
                    attempts = 0
                )
            ),
            scan = ScanProperties(
                blockPublish = BlockPublishProperties(
                    enabled = false
                )
            ),
            job = JobProperties(),
            monitoring = MonitoringProperties(),
            daemon = DaemonWorkerProperties()
        )

        coEvery { blockEventConsumer.start(any()) } returns Unit

        createBlockchainScanner(properties).scan()

        coVerify(exactly = 1) { blockEventConsumer.start(any()) }
        coVerify(exactly = 0) { blockchainClient.newBlocks }
    }

    private fun createBlockchainScanner(properties: TestBlockchainScannerProperties): TestBlockchainScanner {
        return TestBlockchainScanner(
            blockchainClient = blockchainClient,
            subscribers = emptyList(),
            blockMapper = TestBlockMapper(),
            blockService = testBlockService,
            logService = testLogService,
            properties = properties,
            blockEventPublisher = blockEventPublisher,
            blockEventConsumer = blockEventConsumer,
            logRecordEventPublisher = logRecordEventPublisher
        )
    }
}
