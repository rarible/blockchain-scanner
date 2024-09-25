package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomBlock
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.publisher.TestLogRecordEventPublisher
import com.rarible.blockchain.scanner.test.reindex.TestSubscriberFilter
import com.rarible.blockchain.scanner.test.repository.TestLogStorage
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class BlockReindexerTest {

    private val blockHandlerFactory =
        mockk<BlockHandlerFactory<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor, TestLogStorage>>()

    private val logHandlerFactory =
        mockk<LogHandlerFactory<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor, TestLogStorage>>()

    private val blockHandler = mockk<BlockHandler<TestBlockchainBlock>>()
    private val logHandler = mockk<LogHandler<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor, TestLogStorage>>()

    private val descriptor1 = TestDescriptor(
        topic = "topic",
        collection = "collection1",
        contracts = emptyList(),
        entityType = TestCustomLogRecord::class.java,
        groupId = "group",
        storage = mockk(),
    )

    private val descriptor2 = descriptor1.copy(collection = "collection2", storage = mockk())

    private val subscriber1 = TestLogEventSubscriber(descriptor1)
    private val subscriber2 = TestLogEventSubscriber(descriptor2)

    @BeforeEach
    fun beforeEach() {
        clearMocks(logHandlerFactory, blockHandlerFactory, blockHandler, logHandler)
    }

    @Test
    fun `reindex - ok`() = runBlocking<Unit> {
        val subscribers = listOf(subscriber1, subscriber2)
        val reindexer = getReindexer(subscribers)

        val startBlock = randomBlock(id = 1)
        val endBlock = randomBlock(id = 100)
        val ranges = flowOf(TypedBlockRange(LongRange(startBlock.id, endBlock.id), true))

        coEvery { logHandlerFactory.create(eq("group"), eq(subscribers), any()) } returns logHandler
        coEvery { blockHandlerFactory.create(listOf(logHandler)) } returns blockHandler
        coEvery { blockHandler.syncBlocks(ranges, startBlock, ScanMode.REINDEX) } returns flowOf(endBlock)

        val result = reindexer.reindex(startBlock, ranges).toList()
        // Just to ensure flow of blocks has been returned
        assertThat(result).isEqualTo(listOf(endBlock))
    }

    @Test
    fun `reindex - ok, with custom publisher`() = runBlocking<Unit> {
        val subscribers = listOf(subscriber1, subscriber2)
        val reindexer = getReindexer(subscribers)

        val startBlock = randomBlock(id = 1)
        val endBlock = randomBlock(id = 100)
        val ranges = flowOf(TypedBlockRange(LongRange(startBlock.id, endBlock.id), true))
        val publisher = TestLogRecordEventPublisher()

        coEvery { logHandlerFactory.create(eq("group"), eq(subscribers), publisher) } returns logHandler
        coEvery { blockHandlerFactory.create(listOf(logHandler)) } returns blockHandler
        coEvery { blockHandler.syncBlocks(ranges, startBlock, ScanMode.REINDEX) } returns flowOf(endBlock)

        val result = reindexer.reindex(
            baseBlock = startBlock,
            blocksRanges = ranges,
            publisher = publisher
        ).toList()
        // Just to ensure flow of blocks has been returned
        assertThat(result).isEqualTo(listOf(endBlock))
    }

    @Test
    fun `reindex - ok, with filter`() = runBlocking<Unit> {
        val subscribers = listOf(subscriber1, subscriber2)
        val reindexer = getReindexer(subscribers)
        val filter = TestSubscriberFilter(setOf(descriptor1.storage))

        val startBlock = randomBlock(id = 1)
        val endBlock = randomBlock(id = 100)
        val ranges = flowOf(TypedBlockRange(LongRange(startBlock.id, endBlock.id), true))

        coEvery { logHandlerFactory.create(eq("group"), eq(listOf(subscriber1)), any()) } returns logHandler
        coEvery { blockHandlerFactory.create(listOf(logHandler)) } returns blockHandler
        coEvery { blockHandler.syncBlocks(ranges, startBlock, ScanMode.REINDEX_PARTIAL) } returns flowOf(endBlock)

        val result = reindexer.reindex(startBlock, ranges, filter).toList()
        // Just to ensure flow of blocks has been returned
        assertThat(result).isEqualTo(listOf(endBlock))
    }

    private fun getReindexer(subscribers: List<TestLogEventSubscriber>) =
        BlockReindexer(
            subscribers,
            blockHandlerFactory,
            logHandlerFactory
        )
}
