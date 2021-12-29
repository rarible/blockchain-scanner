package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.event.block.BlockStatus
import com.rarible.blockchain.scanner.event.block.toBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.StableBlockEvent
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.buildBlockchain
import com.rarible.blockchain.scanner.test.data.randomBlockchain
import com.rarible.blockchain.scanner.test.handler.TestBlockEventListener
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@IntegrationTest
class BlockHandlerIt : AbstractIntegrationTest() {

    @Test
    fun `start from empty state`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(1)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler.onNewBlock(blockchain.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten())
            .isEqualTo(blockchain.map { it.asNewEvent() })
    }

    @Test
    fun `sync missing blocks`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(10)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler.onNewBlock(blockchain[5])
        assertThat(getAllBlocks()).isEqualTo(blockchain.take(6).map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(blockchain.take(6).map { it.asNewEvent() })

        blockEventListener.blockEvents.clear()
        blockHandler.onNewBlock(blockchain.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(blockchain.drop(6).map { it.asNewEvent() })
    }

    @Test
    fun `chain reorganized`() = runBlocking<Unit> {
        val blockchain1 = randomBlockchain(10)
        val blockEventListener = TestBlockEventListener()
        val blockHandler1 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain1, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler1.onNewBlock(blockchain1.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain1.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(blockchain1.map { it.asNewEvent() })

        val blockchain2 = buildBlockchain(blockchain1.take(5) + randomBlockchain(10).drop(5))
        val blockHandler2 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain2, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )

        blockEventListener.blockEvents.clear()
        blockHandler2.onNewBlock(blockchain2.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain2.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(
            blockchain1.drop(5).reversed().map { it.asRevertEvent() } +
                    blockchain2.drop(5).map { it.asNewEvent() }
        )
    }

    @Test
    fun `reorg happens while syncing`() = runBlocking<Unit> {
        val blockchain1 = randomBlockchain(10)
        val blockEventListener = TestBlockEventListener()
        val blockHandler1 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain1, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler1.onNewBlock(blockchain1[5])

        val blockchain2 = blockchain1.take(7) + randomBlockchain(10).drop(7)
        val blockHandler2 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain2, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )

        blockEventListener.blockEvents.clear()
        blockHandler2.onNewBlock(blockchain2.last())
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(listOf(blockchain1[6].asNewEvent()))
    }

    @Test
    fun `do not emit events if block is already processed`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(10)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler.onNewBlock(blockchain.last())
        assertThat(blockEventListener.blockEvents).isNotEmpty

        blockEventListener.blockEvents.clear()
        blockHandler.onNewBlock(blockchain.last())
        assertThat(blockEventListener.blockEvents).isEmpty()

        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
    }

    @Test
    fun `exception on processing saves pending block`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(10)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val exceptionListener = object : BlockEventListener {
            override suspend fun process(events: List<BlockEvent>) {
                if (events.any { it.number == 5L }) {
                    throw RuntimeException()
                }
            }
        }
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(exceptionListener),
            batchLoad = BlockBatchLoadProperties()
        )
        assertThrows<RuntimeException> { blockHandler.onNewBlock(blockchain.last()) }
        assertThat(getAllBlocks()).isEqualTo(
            blockchain.take(5).map { it.toBlock(BlockStatus.SUCCESS) } +
                    listOf(blockchain[5].toBlock(BlockStatus.PENDING))
        )
    }

    @Test
    fun `process pending log after restart`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(10)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler.onNewBlock(blockchain[4])
        blockService.save(blockchain[5].toBlock(BlockStatus.PENDING))

        blockEventListener.blockEvents.clear()
        blockHandler.onNewBlock(blockchain[6])
        assertThat(blockEventListener.blockEvents.flatten())
            .isEqualTo(listOf(blockchain[5].asNewEvent(), blockchain[6].asNewEvent()))
    }

    @Test
    fun `batch load blocks`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(100)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties(enabled = true, confirmationBlockDistance = 10, batchSize = 10)
        )
        blockHandler.onNewBlock(blockchain.last())
        val blockEvents = blockEventListener.blockEvents.flatten()

        assertThat(blockEvents).hasSize(101)
        assertThat(blockEvents.map { it.number }).isEqualTo((0L..100L).toList())
        assertThat(blockEvents.first()).isInstanceOf(NewBlockEvent::class.java)
        assertThat(blockEvents.drop(1).take(90)).allSatisfy {
            assertThat(it).isInstanceOf(StableBlockEvent::class.java)
        }
        assertThat(blockEvents.drop(1).drop(90)).allSatisfy {
            assertThat(it).isInstanceOf(NewBlockEvent::class.java)
        }
    }
}
