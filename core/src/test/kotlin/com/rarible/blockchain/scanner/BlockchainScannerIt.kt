package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlockchain
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.model.revert
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.blockchain.scanner.test.subscriber.TestTransactionEventSubscriber
import com.rarible.core.common.EventTimeMarks
import com.rarible.core.common.nowMillis
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.time.Duration

@IntegrationTest
class BlockchainScannerIt : AbstractIntegrationTest() {

    private val descriptor by lazy {
        testDescriptor(
            topic = randomString(),
            collection = randomString(),
            contracts = listOf(randomString(), randomString()),
        )
    }

    @Test
    fun `new block - single`() = runBlocking<Unit> {
        val blocks = randomBlockchain(1).map {
            it.withReceivedTime(nowMillis().plusSeconds(1))
        }
        val block0 = blocks[0]
        val block1 = blocks[1]

        val log = randomOriginalLog(block = block1, topic = descriptor.topic, logIndex = 1)
        // log2 should be filtered
        val logFiltered = randomOriginalLog(block = block1, topic = descriptor.topic, logIndex = 1)

        val testBlockchainData = TestBlockchainData(
            blocks = blocks,
            logs = listOf(log, logFiltered),
            newBlocks = blocks
        )

        val subscriber = TestLogEventSubscriber(descriptor)
        val transactionSubscriber = TestTransactionEventSubscriber()
        val blockScanner = createBlockchainScanner(
            testBlockchainClient = TestBlockchainClient(testBlockchainData),
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(transactionSubscriber)
        )
        blockScanner.scan(once = true)

        assertThat(findBlock(block0.number)!!.copy(stats = null)).isEqualTo(block0.toBlock(BlockStatus.SUCCESS))
        assertThat(findBlock(block1.number)!!.copy(stats = null)).isEqualTo(block1.toBlock(BlockStatus.SUCCESS))

        assertPublishedLogRecords(
            descriptor.groupId,
            listOf(
                LogRecordEvent(
                    record = subscriber.getReturnedRecords(block1, log).single(),
                    reverted = false,
                    eventTimeMarks = EventTimeMarks("test")
                )
            ),
            block0
        )

        assertPublishedTransactionRecords(
            "test",
            listOf(
                transactionSubscriber.getExpected(block0),
                transactionSubscriber.getExpected(block1),
            )
        )
    }

    @Test
     fun `new block - retried`() = runBlocking<Unit> {
        val blocks = randomBlockchain(2).map {
            it.withReceivedTime(nowMillis().plusSeconds(1))
        }
        val block0 = blocks[0]
        val block1 = blocks[1]
        val block2 = blocks[2]

        val log = randomOriginalLog(block = block1, topic = descriptor.topic, logIndex = 1)
        // log2 should be filtered
        val logFiltered = randomOriginalLog(block = block1, topic = descriptor.topic, logIndex = 1)

        val testBlockchainData = TestBlockchainData(
            blocks = blocks,
            logs = listOf(log, logFiltered),
            newBlocks = blocks
        )

        val subscriber = TestLogEventSubscriber(descriptor, exceptionProvider = ThrowOnce(IOException("log exception")))
        val transactionSubscriber = TestTransactionEventSubscriber(exceptionProvider = ThrowOnce(IOException("transaction exception")))
        val blockScanner = createBlockchainScanner(
            testBlockchainClient = TestBlockchainClient(testBlockchainData),
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(transactionSubscriber),
            scanRetryProperties = ScanRetryPolicyProperties(
                reconnectDelay = Duration.ofMillis(1),
                reconnectAttempts = 3
            )
        )

        // Throws on scan ending
        assertThrows<java.lang.IllegalStateException> {
            blockScanner.scan()
        }

        assertThat(findBlock(block0.number)!!.copy(stats = null)).isEqualTo(block0.toBlock(BlockStatus.SUCCESS))
        assertThat(findBlock(block1.number)!!.copy(stats = null)).isEqualTo(block1.toBlock(BlockStatus.SUCCESS))
        assertThat(findBlock(block2.number)!!.copy(stats = null)).isEqualTo(block2.toBlock(BlockStatus.SUCCESS))

        assertPublishedLogRecords(
            descriptor.groupId,
            listOf(
                LogRecordEvent(
                    record = subscriber.getReturnedRecords(block1, log).single(),
                    reverted = false,
                    eventTimeMarks = EventTimeMarks("test")
                )
            ),
            block0
        )

        assertPublishedTransactionRecords(
            "test",
            listOf(
                transactionSubscriber.getExpected(block0),
                transactionSubscriber.getExpected(block1),
                transactionSubscriber.getExpected(block2),
            )
        )
    }

    @Test
    fun `new block - 2 subscribers, one failed, block saved`() = runBlocking<Unit> {
        val descriptor1 = testDescriptor(
            topic = "topic",
            collection = "collection1",
            groupId = "group1",
        )
        val descriptor2 = testDescriptor(
            topic = "topic",
            collection = "collection2",
            groupId = "group2",
        )

        val subscriber1 = TestLogEventSubscriber(descriptor1)
        val subscriber2 = TestLogEventSubscriber(descriptor2, exceptionProvider = { RuntimeException("") })
        val transactionSubscriber = TestTransactionEventSubscriber()

        val blocks = randomBlockchain(0)
        val block0 = blocks[0]

        val log = randomOriginalLog(block = block0, topic = "topic", logIndex = 1)

        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = blocks,
                logs = listOf(log),
                newBlocks = blocks
            )
        )

        val blockchainScanner = createBlockchainScanner(
            testBlockchainClient = testBlockchainClient,
            subscribers = listOf(subscriber1, subscriber2),
            transactionSubscribers = listOf(transactionSubscriber),
            scanProperties = ScanProperties(maxFailedBlocksAllowed = 10)
        )

        blockchainScanner.scan(once = true)

        assertPublishedLogRecords(
            "group1",
            listOf(
                subscriber1.getExpected(block0, log)
            )
        )

        // Nothing should be published, this subscriber failed
        assertPublishedLogRecords("group2", emptyList())

        assertPublishedTransactionRecords(
            "test",
            listOf(
                transactionSubscriber.getExpected(blocks[0]),
            )
        )
        assertThat(blockService.getBlock(0L)!!.status).isEqualTo(BlockStatus.ERROR)
    }

    @Test
    fun `new block - 2 subscribers, one failed, too many failed blocks`() = runBlocking<Unit> {
        val descriptor1 = testDescriptor(
            topic = "topic",
            collection = "collection1",
            groupId = "group1",
        )
        val descriptor2 = testDescriptor(
            topic = "topic",
            collection = "collection2",
            groupId = "group2",
        )

        val subscriber1 = TestLogEventSubscriber(descriptor1)
        val subscriber2 = TestLogEventSubscriber(descriptor2, exceptionProvider = { RuntimeException("") })
        val transactionSubscriber = TestTransactionEventSubscriber()

        val blocks = randomBlockchain(2)
        val block0 = blocks[0]

        val log0 = randomOriginalLog(block = blocks[0], topic = "topic", logIndex = 1)
        val log1 = randomOriginalLog(block = blocks[1], topic = "topic", logIndex = 1)
        val log2 = randomOriginalLog(block = blocks[2], topic = "topic", logIndex = 1)

        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = blocks,
                logs = listOf(log0, log1, log2),
                newBlocks = blocks
            )
        )

        val blockchainScanner = createBlockchainScanner(
            testBlockchainClient = testBlockchainClient,
            subscribers = listOf(subscriber1, subscriber2),
            transactionSubscribers = listOf(transactionSubscriber),
            scanProperties = ScanProperties(maxFailedBlocksAllowed = 1)
        )

        assertThrows<IllegalStateException> { blockchainScanner.scan(once = true) }

        assertPublishedLogRecords(
            "group1",
            listOf(
                subscriber1.getExpected(block0, log0)
            )
        )

        // Nothing should be published, this subscriber failed
        assertPublishedLogRecords("group2", emptyList())

        assertPublishedTransactionRecords(
            "test",
            listOf(
                transactionSubscriber.getExpected(blocks[0]),
            )
        )
        // Saved as ERROR since 1 failed block is allowed
        assertThat(blockService.getBlock(0L)!!.status).isEqualTo(BlockStatus.ERROR)
        // Saved as PENDING, will be re-indexed again during next try
        assertThat(blockService.getBlock(1L)!!.status).isEqualTo(BlockStatus.PENDING)
        // Not event reached, scan interrupted at block 1
        assertThat(blockService.getBlock(2L)).isNull()
    }

    @Test
    fun `new block - subscriber failed with IOException`() = runBlocking<Unit> {
        val descriptor = testDescriptor(
            topic = "topic",
            collection = "collection2",
            groupId = "group",
        )

        val subscriber = TestLogEventSubscriber(
            descriptor = descriptor,
            exceptionProvider = { IOException("") }
        )

        val blocks = randomBlockchain(2)
        val log0 = randomOriginalLog(block = blocks[0], topic = "topic", logIndex = 1)
        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = blocks,
                logs = listOf(log0),
                newBlocks = blocks
            )
        )

        val blockchainScanner = createBlockchainScanner(
            testBlockchainClient = testBlockchainClient,
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(),
            scanProperties = ScanProperties(maxFailedBlocksAllowed = 5)
        )

        assertThrows<IOException> { blockchainScanner.scan(once = true) }

        // Nothing should be published, this subscriber failed
        assertPublishedLogRecords("group", emptyList())

        // Not saved due to error
        assertThat(blockService.getBlock(0L)).isNull()
        assertThat(blockService.getBlock(1L)).isNull()
    }

    @Test
    fun `new block - 2 subscribers - different descriptors with the same group`() = runBlocking<Unit> {
        val groupId = "group"
        val descriptor1 = testDescriptor(
            topic = "topic",
            collection = "collection1",
            groupId = groupId,
        )
        val descriptor2 = testDescriptor(
            topic = "topic",
            collection = "collection2",
            groupId = groupId,
        )

        val subscriber1 = TestLogEventSubscriber(descriptor1)
        val subscriber2 = TestLogEventSubscriber(descriptor2)
        val transactionSubscriber1 = TestTransactionEventSubscriber()
        val transactionSubscriber2 = TestTransactionEventSubscriber()

        val blocks = randomBlockchain(2)
        val block1 = blocks[1]
        val block2 = blocks[2]

        val log11 = randomOriginalLog(block = block1, topic = "topic", logIndex = 1)
        val log21 = randomOriginalLog(block = block2, topic = "topic", logIndex = 1)

        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = blocks,
                logs = listOf(log11, log21),
                newBlocks = blocks
            )
        )

        val blockchainScanner = createBlockchainScanner(
            testBlockchainClient = testBlockchainClient,
            subscribers = listOf(subscriber1, subscriber2),
            transactionSubscribers = listOf(transactionSubscriber1, transactionSubscriber2)
        )
        blockchainScanner.scan(once = true)

        assertPublishedLogRecords(
            groupId,
            listOf(
                subscriber1.getExpected(block1, log11),
                subscriber2.getExpected(block1, log11),
                subscriber1.getExpected(block2, log21),
                subscriber2.getExpected(block2, log21),
            )
        )

        assertPublishedTransactionRecords(
            "test",
            listOf(
                transactionSubscriber1.getExpected(blocks[0]),
                transactionSubscriber2.getExpected(blocks[0]),
                transactionSubscriber1.getExpected(block1),
                transactionSubscriber2.getExpected(block1),
                transactionSubscriber1.getExpected(block2),
                transactionSubscriber2.getExpected(block2),
            )
        )
    }

    @Test
    fun `new block - 2 subscribers, log save disabled`() = runBlocking<Unit> {
        val groupId = descriptor.groupId
        val descriptor1 = testDescriptor(
            topic = descriptor.topic,
            collection = randomString(),
            contracts = descriptor.contracts,
        )
        val descriptor2 = testDescriptor(
            topic = descriptor.topic,
            collection = randomString(),
            contracts = descriptor.contracts,
        ).copy(saveLogs = false)

        val subscriber1 = TestLogEventSubscriber(descriptor1)
        val subscriber2 = TestLogEventSubscriber(descriptor2)

        val blocks = randomBlockchain(2)
        val block1 = blocks[1]
        val block2 = blocks[2]

        val log11 = randomOriginalLog(block = block1, topic = descriptor.topic, logIndex = 1)
        val log21 = randomOriginalLog(block = block2, topic = descriptor.topic, logIndex = 1)

        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = blocks,
                logs = listOf(log11, log21),
                newBlocks = blocks
            )
        )

        val blockchainScanner = createBlockchainScanner(
            testBlockchainClient = testBlockchainClient,
            subscribers = listOf(subscriber1, subscriber2),
            transactionSubscribers = listOf()
        )
        blockchainScanner.scan(once = true)

        assertPublishedLogRecords(
            groupId,
            listOf(
                subscriber1.getExpected(block1, log11),
                subscriber2.getExpected(block1, log11),
                subscriber1.getExpected(block2, log21),
                subscriber2.getExpected(block2, log21),
            )
        )

        assertThat(descriptor1.storage.findAllLogs()).hasSize(2)
        assertThat(descriptor2.storage.findAllLogs()).isEmpty()
    }

    @Test
    fun `revert block - single`() = runBlocking<Unit> {
        val blocks = randomBlockchain(2)

        val log11 = randomOriginalLog(block = blocks[1], topic = descriptor.topic, logIndex = 1)
        val log21 = randomOriginalLog(block = blocks[2], topic = descriptor.topic, logIndex = 1)
        val log22 = randomOriginalLog(block = blocks[2], topic = descriptor.topic, logIndex = 2)

        val testBlockchainData = TestBlockchainData(
            blocks = blocks,
            logs = listOf(log11, log21, log22),
            newBlocks = blocks
        )

        val subscriber = TestLogEventSubscriber(descriptor)
        val transactionSubscriber = TestTransactionEventSubscriber()
        val blockScanner = createBlockchainScanner(
            testBlockchainClient = TestBlockchainClient(testBlockchainData),
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(transactionSubscriber),
        )
        blockScanner.scan(once = true)
        assertThat(findBlock(blocks[1].number)!!.copy(stats = null)).isEqualTo(blocks[1].toBlock(BlockStatus.SUCCESS))
        assertThat(findBlock(blocks[2].number)!!.copy(stats = null)).isEqualTo(blocks[2].toBlock(BlockStatus.SUCCESS))
        val confirmedLogs = listOf(
            subscriber.getExpected(blocks[1], log11),
            subscriber.getExpected(blocks[2], log21),
            subscriber.getExpected(blocks[2], log22)
        )
        val confirmedTransactions = blocks.map { transactionSubscriber.getExpected(it) }

        assertPublishedLogRecords(descriptor.groupId, confirmedLogs)
        assertPublishedTransactionRecords("test", confirmedTransactions)

        // Revert the block #2
        val newBlock2 = randomBlockchainBlock(number = blocks[2].number, parentHash = blocks[1].hash)
        val newBlocks = listOf(blocks[0], blocks[1], newBlock2)
        val newLog21 = randomOriginalLog(block = newBlock2, topic = descriptor.topic, logIndex = 1)
        val newLog22 = randomOriginalLog(block = newBlock2, topic = descriptor.topic, logIndex = 2)
        val newTestBlockchainData = TestBlockchainData(
            blocks = newBlocks,
            logs = listOf(log11, newLog21, newLog22),
            newBlocks = listOf(newBlock2)
        )
        val newBlockScanner = createBlockchainScanner(
            testBlockchainClient = TestBlockchainClient(newTestBlockchainData),
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(transactionSubscriber),
        )
        newBlockScanner.scan(once = true)

        val revertedRecord22 = subscriber.getReturnedRecords(blocks[2], log22).single().revert()
        val revertedRecord21 = subscriber.getReturnedRecords(blocks[2], log21).single().revert()

        val newRecord21 = subscriber.getReturnedRecords(newBlock2, newLog21).single()
        val newRecord22 = subscriber.getReturnedRecords(newBlock2, newLog22).single()

        assertPublishedLogRecords(
            descriptor.groupId,
            confirmedLogs + listOf(
                LogRecordEvent(record = revertedRecord22, reverted = true, eventTimeMarks = EventTimeMarks("test")),
                LogRecordEvent(record = revertedRecord21, reverted = true, eventTimeMarks = EventTimeMarks("test")),
                LogRecordEvent(record = newRecord21, reverted = false, eventTimeMarks = EventTimeMarks("test")),
                LogRecordEvent(record = newRecord22, reverted = false, eventTimeMarks = EventTimeMarks("test"))
            )
        )

        assertPublishedTransactionRecords(
            "test",
            confirmedTransactions + listOf(transactionSubscriber.getExpected(newBlock2))
        )

        val savedLog21 = descriptor.storage.findLogEvent(newRecord21.id)
        assertThat(savedLog21?.log?.reverted).isFalse
        val savedLog22 = descriptor.storage.findLogEvent(newRecord22.id)
        assertThat(savedLog22?.log?.reverted).isFalse

        val savedRevertedLog22 = descriptor.storage.findLogEvent(revertedRecord22.id)
        assertThat(savedRevertedLog22?.log?.reverted).isTrue
        val savedRevertedLog21 = descriptor.storage.findLogEvent(revertedRecord21.id)
        assertThat(savedRevertedLog21?.log?.reverted).isTrue
    }

    @Test
    fun `revert logs of pending block after restart but then process them again`() = runBlocking<Unit> {
        val blocks = randomBlockchain(2)

        val log11 = randomOriginalLog(block = blocks[1], topic = descriptor.topic, logIndex = 1)
        val log12 = randomOriginalLog(block = blocks[1], topic = descriptor.topic, logIndex = 2)
        val log21 = randomOriginalLog(block = blocks[2], topic = descriptor.topic, logIndex = 1)

        val testBlockchainData = TestBlockchainData(
            blocks = blocks,
            logs = listOf(log11, log12, log21),
            newBlocks = blocks
        )

        val subscriber = TestLogEventSubscriber(descriptor)
        val transactionSubscriber = TestTransactionEventSubscriber()
        val blockScanner = createBlockchainScanner(
            // Process only the blocks #0 and #1.
            testBlockchainClient = TestBlockchainClient(testBlockchainData.copy(newBlocks = blocks.take(2))),
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(transactionSubscriber),
        )
        blockScanner.scan(once = true)

        assertThat(findBlock(blocks[1].number)!!.copy(stats = null)).isEqualTo(blocks[1].toBlock(BlockStatus.SUCCESS))
        val confirmedLogs = listOf(
            subscriber.getExpected(blocks[1], log11),
            subscriber.getExpected(blocks[1], log12),
        )
        val confirmedTransactions = listOf(
            transactionSubscriber.getExpected(blocks[0]),
            transactionSubscriber.getExpected(blocks[1]),
        )
        assertPublishedLogRecords(descriptor.groupId, confirmedLogs)
        assertPublishedTransactionRecords("test", confirmedTransactions)

        // Imitate the block #1 is PENDING.
        blockService.save(blocks[1].toBlock(BlockStatus.PENDING))
        val newBlockchainScanner = createBlockchainScanner(
            // Process all the blocks
            testBlockchainClient = TestBlockchainClient(testBlockchainData),
            subscribers = listOf(subscriber),
            transactionSubscribers = listOf(transactionSubscriber),
        )
        newBlockchainScanner.scan(once = true)

        // Block #1 was PENDING, so we, firstly, revert all possibly saved logs and then apply them again.
        assertPublishedLogRecords(
            descriptor.groupId,
            confirmedLogs + listOf(
                subscriber.getExpected(blocks[1], log12, true),
                subscriber.getExpected(blocks[1], log11, true),
                subscriber.getExpected(blocks[1], log11),
                subscriber.getExpected(blocks[1], log12),
                subscriber.getExpected(blocks[2], log21),
            )
        )
        assertPublishedTransactionRecords(
            "test",
            confirmedTransactions + listOf(
                transactionSubscriber.getExpected(blocks[1]),
                transactionSubscriber.getExpected(blocks[2]),
            )
        )
    }

    private fun assertPublishedLogRecords(
        groupId: String,
        expectedEvents: List<LogRecordEvent>,
        trigger: TestBlockchainBlock? = null
    ) {
        val publishedByGroup = testLogRecordEventPublisher.publishedLogRecords[groupId] ?: emptyList()
        for (i in expectedEvents.indices) {
            val published = publishedByGroup[i]
            val expected = expectedEvents[i]
            assertThat(published.record).isEqualTo(expected.record)
            assertThat(published.reverted).isEqualTo(expected.reverted)

            val marks = published.eventTimeMarks.marks
            assertThat(marks).hasSize(4)
            assertThat(marks[0].name).isEqualTo("source")
            assertThat(marks[1].name).isEqualTo("source-out")
            assertThat(marks[2].name).isEqualTo("scanner-in")
            assertThat(marks[3].name).isEqualTo("scanner-out")

            if (trigger != null) {
                assertThat(marks[1].date.epochSecond).isEqualTo(trigger.receivedTime.epochSecond)
            }
        }
    }

    private fun assertPublishedTransactionRecords(groupId: String, expectedEvents: List<TransactionRecordEvent>) {
        val publishedByGroup = testTransactionRecordEventPublisher.publishedTransactionRecords[groupId] ?: emptyList()
        assertThat(publishedByGroup).hasSize(expectedEvents.size)
        for (i in expectedEvents.indices) {
            val published = publishedByGroup[i]
            val expected = expectedEvents[i]
            assertThat(published.record).isEqualTo(expected.record)
            assertThat(published.reverted).isEqualTo(expected.reverted)

            val marks = published.eventTimeMarks.marks
            assertThat(marks).hasSize(4)
            assertThat(marks[0].name).isEqualTo("source")
            assertThat(marks[1].name).isEqualTo("source-out")
            assertThat(marks[2].name).isEqualTo("scanner-in")
            assertThat(marks[3].name).isEqualTo("scanner-out")
        }
    }

    fun TestTransactionEventSubscriber.getExpected(
        block: TestBlockchainBlock,
        reverted: Boolean = false
    ) = TransactionRecordEvent(
        record = getReturnedRecords(block).single(),
        reverted = reverted,
        eventTimeMarks = EventTimeMarks("test")
    )

    fun TestLogEventSubscriber.getExpected(
        block: TestBlockchainBlock,
        original: TestOriginalLog,
        reverted: Boolean = false
    ) = LogRecordEvent(
        record = getReturnedRecords(block, original).single().let { if (reverted) it.revert() else it },
        reverted = reverted,
        eventTimeMarks = EventTimeMarks("test")
    )
}

class ThrowOnce(private val exception: Exception) : () -> Exception? {
    private var invoked: Boolean = false
    override fun invoke(): Exception? = exception.takeIf { !invoked }?.also { invoked = true }
}
