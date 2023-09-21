package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.buildBlockchain
import com.rarible.blockchain.scanner.test.data.randomBlockchain
import com.rarible.blockchain.scanner.test.handler.TestBlockEventListener
import io.mockk.mockk
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler.onNewBlock(blockchain.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents)
            .isEqualTo(
                listOf(
                    listOf(blockchain[0].asNewStableEvent()),
                    blockchain.drop(1).map { it.asNewUnstableEvent() }
                )
            )
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler.onNewBlock(blockchain[5])
        assertThat(getAllBlocks()).isEqualTo(blockchain.take(6).map { it.toBlock(BlockStatus.SUCCESS) })
        val blockEvents = blockEventListener.blockEvents
        assertThat(blockEvents).isEqualTo(
            listOf(
                listOf(blockchain[0].asNewStableEvent()),
                blockchain.drop(1).take(5).map { it.asNewUnstableEvent() }
            )
        )

        blockEvents.clear()
        blockHandler.onNewBlock(blockchain.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEvents.flatten()).isEqualTo(
            blockchain.drop(6).map { it.asNewUnstableEvent() }
        )
    }

    @Test
    fun `chain reorganized`() = runBlocking<Unit> {
        val blockchain1 = randomBlockchain(10)
        val blockEventListener = TestBlockEventListener()
        val blockHandler1 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain1, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler1.onNewBlock(blockchain1.last())
        assertThat(getAllBlocks()).isEqualTo(blockchain1.map { it.toBlock(BlockStatus.SUCCESS) })
        assertThat(blockEventListener.blockEvents)
            .isEqualTo(
                listOf(
                    listOf(blockchain1[0].asNewStableEvent()),
                    blockchain1.drop(1).map { it.asNewUnstableEvent() }
                )
            )

        val blockchain2 = buildBlockchain(blockchain1.take(5) + randomBlockchain(10).drop(5))
        val blockHandler2 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain2, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler1.onNewBlock(blockchain1[5])

        val blockchain2 = blockchain1.take(7) + randomBlockchain(10).drop(7)
        val blockHandler2 = BlockHandler(
            blockClient = TestBlockchainClient(TestBlockchainData(blockchain2, emptyList(), emptyList())),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler.onNewBlock(blockchain.last())
        assertThat(blockEventListener.blockEvents).isNotEmpty

        blockEventListener.blockEvents.clear()
        blockHandler.onNewBlock(blockchain.last())
        assertThat(blockEventListener.blockEvents).isEmpty()

        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
    }

    @Test
    fun `should save stable block on reindex`() = runBlocking<Unit> {
        val syncBlocks = randomBlockchain(10).toMutableList()
        val baseBlock = syncBlocks.first()
        blockService.save(baseBlock.toBlock())

        val range = flow { emit(TypedBlockRange(LongRange(1, 10), true)) }
        val testBlockchainData = TestBlockchainData(syncBlocks - baseBlock, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler.syncBlocks(range, baseBlock.toBlock(), resyncStable = false).toList()
        assertThat(blockService.getBlock(10)).isNotNull
    }

    @Test
    fun `should save missing stable block on reindex`() = runBlocking<Unit> {
        val syncBlocks = randomBlockchain(10).toMutableList()
        val baseBlock = syncBlocks.first()
        blockService.insertAll(syncBlocks.subList(0, 5).map { it.toBlock() })

        val range = flow { emit(TypedBlockRange(LongRange(1, 10), true)) }
        val testBlockchainData = TestBlockchainData(syncBlocks - baseBlock, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        blockHandler.syncBlocks(range, baseBlock.toBlock(), resyncStable = true).toList()
        // Missing block, should be added
        assertThat(blockService.getBlock(10)).isNotNull
        // Existing block, should stay the same
        assertThat(blockService.getBlock(2)).isNotNull
    }

    @Test
    fun `exception on processing saves pending blocks`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(10)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val exceptionListener = object : BlockEventListener<TestBlockchainBlock> {
            override suspend fun process(events: List<BlockEvent<TestBlockchainBlock>>): BlockEventListener.Result {
                if (events.any { it.number == 5L }) {
                    throw RuntimeException()
                }
                return BlockEventListener.Result.EMPTY
            }
        }
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(exceptionListener),
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
        )
        assertThrows<RuntimeException> { blockHandler.onNewBlock(blockchain.last()) }
        assertThat(getAllBlocks()).isEqualTo(
            blockchain.map { it.toBlock(if (it.number == 0L) BlockStatus.SUCCESS else BlockStatus.PENDING) }
        )
    }

    @Test
    fun `exception on batch processing saves pending blocks`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(100)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val exceptionListener = object : BlockEventListener<TestBlockchainBlock> {
            override suspend fun process(events: List<BlockEvent<TestBlockchainBlock>>): BlockEventListener.Result {
                if (events.any { it.number == 55L }) {
                    throw RuntimeException()
                }
                return BlockEventListener.Result.EMPTY
            }
        }
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(exceptionListener),
            scanProperties = ScanProperties(
                batchLoad = BlockBatchLoadProperties(
                    confirmationBlockDistance = 10,
                    batchSize = 10
                )
            ),
            monitor = mockk(relaxed = true)
        )
        assertThrows<RuntimeException> { blockHandler.onNewBlock(blockchain.last()) }
        assertThat(getAllBlocks()).isEqualTo(
            blockchain.take(51).map { it.toBlock(BlockStatus.SUCCESS) }
        )

        // After restart, collect the events.
        val testBlockEventListener = TestBlockEventListener()
        val newBlockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(testBlockEventListener),
            scanProperties = ScanProperties(
                batchLoad = BlockBatchLoadProperties(
                    confirmationBlockDistance = 10,
                    batchSize = 10
                )
            ),
            monitor = mockk(relaxed = true)
        )
        newBlockHandler.onNewBlock(blockchain.last())
        assertThat(testBlockEventListener.blockEvents).isEqualTo(
            blockchain.subList(51, 91).map { it.asNewStableEvent() }.chunked(10) +
                listOf(blockchain.drop(91).map { it.asNewUnstableEvent() })
        )
        assertThat(getAllBlocks()).isEqualTo(blockchain.map { it.toBlock(BlockStatus.SUCCESS) })
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
            scanProperties = ScanProperties(),
            monitor = mockk(relaxed = true)
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
            scanProperties = ScanProperties(
                batchLoad = BlockBatchLoadProperties(
                    confirmationBlockDistance = 10,
                    batchSize = 10
                )
            ),
            monitor = mockk(relaxed = true)
        )
        blockHandler.onNewBlock(blockchain.last())
        val blockBatchEvents = blockEventListener.blockEvents

        assertThat(blockBatchEvents)
            .isEqualTo(
                listOf(listOf(blockchain[0].asNewStableEvent())) +
                    blockchain.subList(1, 91).map { it.asNewStableEvent() }.chunked(10) +
                    listOf(blockchain.subList(91, 101).map { it.asNewUnstableEvent() })
            )
    }

    @Test
    fun `process pending after restart - firstly revert - then process`() = runBlocking<Unit> {
        val blockchain = randomBlockchain(100)
        val testBlockchainData = TestBlockchainData(blockchain, emptyList(), emptyList())
        val blockEventListener = TestBlockEventListener()
        val blockHandler = BlockHandler(
            blockClient = TestBlockchainClient(testBlockchainData),
            blockService = blockService,
            blockEventListeners = listOf(blockEventListener),
            scanProperties = ScanProperties(
                batchLoad = BlockBatchLoadProperties(
                    confirmationBlockDistance = 10,
                    batchSize = 10
                )
            ),
            monitor = mockk(relaxed = true)
        )
        // Process blocks #0, #1, #2, #3, #4
        blockHandler.onNewBlock(blockchain[4])

        // Mark block #5 as PENDING.
        blockService.save(blockchain[5].toBlock(BlockStatus.PENDING))
        blockEventListener.blockEvents.clear()

        // Process the remaining blocks #6, #7 ... #100
        blockHandler.onNewBlock(blockchain.last())
        val blockEvents = blockEventListener.blockEvents

        assertThat(blockEvents).isEqualTo(
            // Repeat the new stable event for block #5.
            listOf(listOf(blockchain[5].asRevertEvent())) +
                blockchain.subList(5, 91).map { it.asNewStableEvent() }.chunked(10) +
                listOf(blockchain.subList(91, 101).map { it.asNewUnstableEvent() })
        )
    }

    private fun TestBlockchainBlock.asNewUnstableEvent() = NewUnstableBlockEvent(this, ScanMode.REALTIME)
    private fun TestBlockchainBlock.asNewStableEvent() = NewStableBlockEvent(this, ScanMode.REALTIME)

    private fun TestBlockchainBlock.asRevertEvent() =
        RevertedBlockEvent<TestBlockchainBlock>(this.number, this.hash, ScanMode.REALTIME)
}
