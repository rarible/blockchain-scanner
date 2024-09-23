package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.BlockScanPlanner
import com.rarible.blockchain.scanner.reindex.LogHandlerFactory
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.stubListenerResult
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
internal class ReconciliationLogHandlerImplTest {

    private val blockchainClient = mockk<BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor>>()
    private val logHandlerFactory = mockk<LogHandlerFactory<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>()

    private val logService = mockk<LogService<TestLogRecord, TestDescriptor>>()
    private val reindexHandler = mockk<BlockReindexer<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>()
    private val handlerPlanner = mockk<BlockScanPlanner<TestBlockchainBlock>>()
    private val monitor = mockk<LogReconciliationMonitor> {
        every { onInconsistency() } returns Unit
    }
    private val descriptor11a = TestDescriptor(
        topic = "topic",
        collection = "collection1",
        contracts = emptyList(),
        entityType = TestCustomLogRecord::class.java,
        groupId = "groupId1",
    )
    private val subscriber11a = TestLogEventSubscriber(descriptor11a)
    private val logHandler11a = mockk<LogHandler<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>(name = "11")

    private val descriptor11b = TestDescriptor(
        topic = "topic",
        collection = "collection1",
        contracts = emptyList(),
        entityType = TestCustomLogRecord::class.java,
        groupId = "groupId1",
    )
    private val subscriber11b = TestLogEventSubscriber(descriptor11b)

    private val descriptor22 = TestDescriptor(
        topic = "topic",
        collection = "collection2",
        contracts = emptyList(),
        entityType = TestCustomLogRecord::class.java,
        groupId = "groupId2",
    )
    private val subscriber22 = TestLogEventSubscriber(descriptor22)
    private val logHandler22 = mockk<LogHandler<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>(name = "22")

    private val descriptor23 = TestDescriptor(
        topic = "topic",
        collection = "collection2",
        contracts = emptyList(),
        entityType = TestCustomLogRecord::class.java,
        groupId = "groupId3",
    )
    private val subscriber23 = TestLogEventSubscriber(descriptor23)
    private val logHandler23 = mockk<LogHandler<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>(name = "23")

    @Test
    fun `check block range - no diff`() = runBlocking<Unit> {
        val handler = create(listOf(subscriber11a, subscriber11b, subscriber22, subscriber23))
        val range = TypedBlockRange(LongRange(10, 11), stable = true)
        val block10 = randomBlockchainBlock(number = 10)
        val block11 = randomBlockchainBlock(number = 11)

        coEvery { logService.countByBlockNumber("collection1", 10) } returns 10
        coEvery { logService.countByBlockNumber("collection2", 10) } returns 11
        coEvery { logService.countByBlockNumber("collection1", 11) } returns 12
        coEvery { logService.countByBlockNumber("collection2", 11) } returns 13

        coEvery { blockchainClient.getBlock(10) } returns block10
        coEvery { blockchainClient.getBlock(11) } returns block11

        every {
            logHandlerFactory.create("groupId1", listOf(subscriber11a, subscriber11b), any(), any())
        } returns logHandler11a
        every { logHandlerFactory.create("groupId2", listOf(subscriber22), any(), any()) } returns logHandler22
        every { logHandlerFactory.create("groupId3", listOf(subscriber23), any(), any()) } returns logHandler23

        // Block 10 statistics
        val block10Handler1Stats = mockk<BlockStats> { every { inserted } returns 10 }
        coEvery {
            logHandler11a.process(listOf(NewStableBlockEvent(block10, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block10.number), block10Handler1Stats)

        val block10Handler2Stats = mockk<BlockStats> { every { inserted } returns 9 }
        coEvery {
            logHandler22.process(listOf(NewStableBlockEvent(block10, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block10.number), block10Handler2Stats)

        val block10Handler3Stats = mockk<BlockStats> { every { inserted } returns 2 }
        coEvery {
            logHandler23.process(listOf(NewStableBlockEvent(block10, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block10.number), block10Handler3Stats)

        // Block 11 statistics
        val block11Handler1Stats = mockk<BlockStats> { every { inserted } returns 12 }
        coEvery {
            logHandler11a.process(listOf(NewStableBlockEvent(block11, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block11.number), block11Handler1Stats)

        val block11Handler2Stats = mockk<BlockStats> { every { inserted } returns 5 }
        coEvery {
            logHandler22.process(listOf(NewStableBlockEvent(block11, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block11.number), block11Handler2Stats)

        val block11Handler3Stats = mockk<BlockStats> { every { inserted } returns 8 }
        coEvery {
            logHandler23.process(listOf(NewStableBlockEvent(block11, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block11.number), block11Handler3Stats)

        val result = handler.check(range, 2)
        assertThat(result).isEqualTo(11)
    }

    @Test
    fun `check block range - reindex`() = runBlocking<Unit> {
        val handler = create(listOf(subscriber11a), reconciliationProperties = ReconciliationProperties(autoReindex = true))
        val range = TypedBlockRange(LongRange(100, 100), stable = true)
        val block100 = randomBlockchainBlock(number = 100)

        coEvery { logService.countByBlockNumber("collection1", 100) } returns 10
        coEvery { blockchainClient.getBlock(100) } returns block100

        every { logHandlerFactory.create("groupId1", listOf(subscriber11a), any(), any()) } returns logHandler11a

        coEvery {
            logHandler11a.process(listOf(NewStableBlockEvent(block100, ScanMode.REINDEX)))
        } returns stubListenerResult(listOf(block100.number))

        val baseBlock = mockk<Block>()
        val planRange = mockk<Flow<TypedBlockRange>>()
        val plan = BlockScanPlanner.ScanPlan(planRange, baseBlock, 100, 100)
        coEvery { handlerPlanner.getPlan(BlockRange(100, null, null)) } returns plan
        coEvery { reindexHandler.reindex(baseBlock, planRange, any(), any()) } returns flowOf(baseBlock)

        val result = handler.check(range, 2)
        assertThat(result).isEqualTo(100)
    }

    private fun create(subscribers: List<TestLogEventSubscriber>, reconciliationProperties: ReconciliationProperties = ReconciliationProperties()): ReconciliationLogHandler {
        return ReconciliationLogHandlerImpl(
            logService = logService,
            reconciliationProperties = reconciliationProperties,
            blockchainClient = blockchainClient,
            logHandlerFactory = logHandlerFactory,
            reindexer = reindexHandler,
            planner = handlerPlanner,
            monitor = monitor,
            subscribers = subscribers)
    }
}