package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.ethereum.handler.HandlerPlanner
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.blockchain.scanner.ethereum.model.BlockRange
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogHandler
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.service.LogHandlerFactory
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.handler.BlocksRange
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test


@ExperimentalCoroutinesApi
internal class ReconciliationLogHandlerTest {
    private val ethereumClient = mockk<EthereumBlockchainClient>()
    private val logRepository = mockk<EthereumLogRepository>()
    private val logHandlerFactory = mockk<LogHandlerFactory>()
    private val logService = mockk<EthereumLogService>()
    private val reindexHandler = mockk<ReindexHandler>()
    private val handlerPlanner = mockk<HandlerPlanner>()
    private val onReconciliationListener = mockk<OnReconciliationListener>()
    private val reconciliationProperties = mockk<ReconciliationProperties>()
    private val monitor = mockk<EthereumLogReconciliationMonitor> {
        every { onInconsistency() } returns Unit
    }
    private val scannerProperties = mockk<EthereumScannerProperties> {
        every { reconciliation } returns reconciliationProperties
    }
    private val descriptor1 = mockk<EthereumDescriptor> {
        every { collection } returns "collection1"
        every { groupId } returns "groupId1"
    }
    private val subscriber1 = mockk<EthereumLogEventSubscriber> {
        every { getDescriptor() } returns descriptor1
    }
    private val logHandler1 = mockk<EthereumLogHandler>()

    private val descriptor2 = mockk<EthereumDescriptor> {
        every { collection } returns "collection1"
        every { groupId } returns "groupId1"
    }
    private val subscriber2 = mockk<EthereumLogEventSubscriber> {
        every { getDescriptor() } returns descriptor2
    }
    private val logHandler2 = mockk<EthereumLogHandler>()

    private val descriptor3 = mockk<EthereumDescriptor> {
        every { collection } returns "collection2"
        every { groupId } returns "groupId2"
    }
    private val subscriber3 = mockk<EthereumLogEventSubscriber> {
        every { getDescriptor() } returns descriptor3
    }

    private val descriptor4 = mockk<EthereumDescriptor> {
        every { collection } returns "collection2"
        every { groupId } returns "groupId3"
    }
    private val subscriber4 = mockk<EthereumLogEventSubscriber> {
        every { getDescriptor() } returns descriptor4
    }
    private val logHandler3 = mockk<EthereumLogHandler>()

    @Test
    fun `check block range - no diff`() = runBlocking<Unit> {
        val handler = create(listOf(subscriber1, subscriber2, subscriber3, subscriber4))
        val range = BlocksRange(LongRange(10, 11), stable = true)
        val block10 = mockk<EthereumBlockchainBlock>()
        val block11 = mockk<EthereumBlockchainBlock>()

        coEvery { logRepository.countByBlockNumber("collection1", 10) } returns 10
        coEvery { logRepository.countByBlockNumber("collection2", 10) } returns 11
        coEvery { logRepository.countByBlockNumber("collection1", 11) } returns 12
        coEvery { logRepository.countByBlockNumber("collection2", 11) } returns 13

        coEvery { ethereumClient.getBlock(10) } returns block10
        coEvery { ethereumClient.getBlock(11) } returns block11

        every { logHandlerFactory.create("groupId1", listOf(subscriber1, subscriber2), any(), any()) } returns logHandler1
        every { logHandlerFactory.create("groupId2", listOf(subscriber3), any(), any()) } returns logHandler2
        every { logHandlerFactory.create("groupId3", listOf(subscriber4), any(), any()) } returns logHandler3

        //Block 10 statistics
        val block10Handler1Stats = mockk< BlockStats> { every { inserted } returns 10 }
        coEvery { logHandler1.process(listOf(NewStableBlockEvent(block10))) } returns mapOf(10L to block10Handler1Stats)

        val block10Handler2Stats = mockk< BlockStats> { every { inserted } returns 9 }
        coEvery { logHandler2.process(listOf(NewStableBlockEvent(block10))) } returns mapOf(10L to block10Handler2Stats)
        val block10Handler3Stats = mockk< BlockStats> { every { inserted } returns 2 }
        coEvery { logHandler3.process(listOf(NewStableBlockEvent(block10))) } returns mapOf(10L to block10Handler3Stats)

        //Block 11 statistics
        val block11Handler1Stats = mockk< BlockStats> { every { inserted } returns 12 }
        coEvery { logHandler1.process(listOf(NewStableBlockEvent(block11))) } returns mapOf(11L to block11Handler1Stats)

        val block11Handler2Stats = mockk< BlockStats> { every { inserted } returns 5 }
        coEvery { logHandler2.process(listOf(NewStableBlockEvent(block11))) } returns mapOf(11L to block11Handler2Stats)
        val block11Handler3Stats = mockk< BlockStats> { every { inserted } returns 8 }
        coEvery { logHandler3.process(listOf(NewStableBlockEvent(block11))) } returns mapOf(11L to block11Handler3Stats)

        val result = handler.check(range, 2)
        assertThat(result).isEqualTo(11)
    }

    @Test
    fun `check block range - reindex`() = runBlocking<Unit> {
        val handler = create(listOf(subscriber1))
        val range = BlocksRange(LongRange(100, 100), stable = true)
        val block100 = mockk<EthereumBlockchainBlock>()

        coEvery { logRepository.countByBlockNumber("collection1", 100) } returns 10
        coEvery { ethereumClient.getBlock(100) } returns block100

        every { logHandlerFactory.create("groupId1", listOf(subscriber1), any(), any()) } returns logHandler1

        //Block 100 statistics
        val block100Stats = mockk< BlockStats> { every { inserted } returns 11 }
        coEvery { logHandler1.process(listOf(NewStableBlockEvent(block100))) } returns mapOf(100L to block100Stats)


        every { reconciliationProperties.autoReindex } returns true
        val baseBlock = mockk<Block>()
        val planRange = mockk<Flow<BlocksRange>>()
        val plan = HandlerPlanner.Plan(planRange, baseBlock)
        coEvery { handlerPlanner.getPlan(BlockRange(100, null, null)) } returns plan
        coEvery { reindexHandler.reindex(baseBlock, planRange) } returns flow { emit(baseBlock) }

        val result = handler.check(range, 2)
        assertThat(result).isEqualTo(100)
    }

    private fun create(subscribers: List<EthereumLogEventSubscriber>): ReconciliationLogHandler {
        return ReconciliationLogHandler(
            ethereumClient = ethereumClient,
            logRepository = logRepository,
            logHandlerFactory = logHandlerFactory,
            logService = logService,
            reindexHandler = reindexHandler,
            handlerPlanner = handlerPlanner,
            onReconciliationListeners = listOf(onReconciliationListener),
            scannerProperties = scannerProperties,
            subscribers = subscribers,
            monitor = monitor
        )
    }
}

