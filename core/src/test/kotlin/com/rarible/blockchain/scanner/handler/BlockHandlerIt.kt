package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
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
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten())
            .isEqualTo(blockchain.map { it.asNewUnstableEvent() })
    }

    @Test
    fun `revert pending block after restart if blockchain reorged`() = runBlocking<Unit> {
        val blockchain1 = randomBlockchain(10)
        val blockEventListener = TestBlockEventListener()
        val testBlockchainData1 = TestBlockchainData(blockchain1, emptyList(), emptyList())
        val blockHandler1 = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData1),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler1.onNewBlock(blockchain1[4])
        blockService.save(blockchain1[5].toBlock(BlockStatus.PENDING))
        blockEventListener.blockEvents.clear()

        val blockchain2 = buildBlockchain(blockchain1.take(5) + randomBlockchain(10).drop(5))
        val testBlockchainData2 = TestBlockchainData(blockchain2, emptyList(), emptyList())
        val blockHandler2 = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData2),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties()
        )
        blockHandler2.onNewBlock(blockchain2[6])

        assertThat(blockEventListener.blockEvents.flatten())
            .isEqualTo(
                listOf(
                    blockchain1[5].asRevertEvent(),
                    blockchain2[5].asNewUnstableEvent(),
                    blockchain2[6].asNewUnstableEvent()
                )
            )
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
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(
            blockchain.take(6).map { it.asNewUnstableEvent() })

        blockEventListener.blockEvents.clear()
        blockHandler.onNewBlock(blockchain.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(
            blockchain.drop(6).map { it.asNewUnstableEvent() })
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
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(blockchain1.map { it.asNewUnstableEvent() })

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
            blockchain1.drop(5).reversed().map { it.asRevertEvent() } + blockchain2.drop(5)
                .map { it.asNewUnstableEvent() }
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
        assertThat(blockEventListener.blockEvents.flatten()).isEqualTo(listOf(blockchain1[6].asNewUnstableEvent()))
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
        val exceptionListener = object : BlockEventListener<TestBlockchainBlock> {
            override suspend fun process(events: List<BlockEvent<TestBlockchainBlock>>) {
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
    fun `revert and then process pending log after restart`() = runBlocking<Unit> {
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
            .isEqualTo(
                listOf(
                    blockchain[5].asRevertEvent(),
                    blockchain[5].asNewUnstableEvent(),
                    blockchain[6].asNewUnstableEvent()
                )
            )
    }

    @Test
    fun `batch sync blocks`() = runBlocking<Unit> {
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
            assertThat(it).isInstanceOf(NewStableBlockEvent::class.java)
        }
        assertThat(blockEvents.drop(1).drop(90)).allSatisfy {
            assertThat(it).isInstanceOf(NewUnstableBlockEvent::class.java)
        }
    }

    @Test
    fun `revert and then process pending block before batch syncing blocks`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(100)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            batchLoad = BlockBatchLoadProperties(enabled = true, confirmationBlockDistance = 10, batchSize = 10)
        )
        // Process blocks #0, #1, #2, #3, #4
        blockHandler.onNewBlock(blockchain[4])

        // Mark block #5 as PENDING.
        blockService.save(blockchain[5].toBlock(BlockStatus.PENDING))
        blockEventListener.blockEvents.clear()

        // Process the remaining blocks #6, #7 ... #100
        blockHandler.onNewBlock(blockchain.last())
        val blockEvents = blockEventListener.blockEvents.flatten()

        assertThat(blockEvents).hasSize(97)
        assertThat(blockEvents.map { it.number }).isEqualTo(listOf(5L) + (5L..100L).toList())
        // The pending block #5 must firstly be reverted and then processed with stable.
        assertThat(blockEvents.first()).isInstanceOf(RevertedBlockEvent::class.java)
        assertThat(blockEvents[1]).isInstanceOf(NewStableBlockEvent::class.java)

        // The blocks #6 ... #90 must be processed with NewStableBlockEvent
        assertThat(blockEvents.drop(2).take(85)).allSatisfy {
            assertThat(it).isInstanceOf(NewStableBlockEvent::class.java)
        }

        // The blocks #91..100 must be processed with NewUnstableBlockEvent
        assertThat(blockEvents.drop(2).drop(85)).allSatisfy {
            assertThat(it).isInstanceOf(NewUnstableBlockEvent::class.java)
        }
    }

    private fun TestBlockchainBlock.asNewUnstableEvent() = NewUnstableBlockEvent(this)

    private fun TestBlockchainBlock.asRevertEvent() = RevertedBlockEvent<TestBlockchainBlock>(this.number, this.hash)
}
