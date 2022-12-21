package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlockchain
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.revert
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventFilter
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@IntegrationTest
class BlockchainScannerIt : AbstractIntegrationTest() {

    private val descriptor = TestDescriptor(
        topic = randomString(),
        collection = randomString(),
        contracts = listOf(randomString(), randomString()),
        entityType = TestCustomLogRecord::class.java
    )

    @Test
    fun `new block - single`() = runBlocking<Unit> {
        val blocks = randomBlockchain(1)
        val block = blocks[1]
        val log = randomOriginalLog(block = block, topic = descriptor.topic, logIndex = 1)
        // log2 should be filtered
        val logFiltered = randomOriginalLog(block = block, topic = descriptor.topic, logIndex = 1)

        val testBlockchainData = TestBlockchainData(
            blocks = blocks,
            logs = listOf(log, logFiltered),
            newBlocks = blocks
        )

        val subscriber = TestLogEventSubscriber(descriptor)
        val blockScanner = createBlockchainScanner(
            TestBlockchainClient(testBlockchainData),
            listOf(subscriber),
            listOf(TestLogEventFilter(setOf(logFiltered.transactionHash)))
        )
        blockScanner.scan(once = true)

        assertThat(findBlock(blocks[0].number)!!.copy(stats = null)).isEqualTo(blocks[0].toBlock(BlockStatus.SUCCESS))
        assertThat(findBlock(block.number)!!.copy(stats = null)).isEqualTo(block.toBlock(BlockStatus.SUCCESS))

        assertThat(testLogRecordEventPublisher.publishedLogRecords).isEqualTo(
            mapOf(
                descriptor.groupId to listOf(
                    LogRecordEvent(
                        record = subscriber.getReturnedRecords(block, log).single(),
                        reverted = false
                    )
                )
            )
        )
    }

    @Test
    fun `new block - 2 subscribers - different descriptors with the same group`() = runBlocking<Unit> {
        val groupId = "group"
        val descriptor1 = TestDescriptor(
            topic = "topic",
            collection = "collection1",
            contracts = emptyList(),
            entityType = TestCustomLogRecord::class.java,
            groupId = groupId
        )
        val descriptor2 = TestDescriptor(
            topic = "topic",
            collection = "collection2",
            contracts = emptyList(),
            entityType = TestCustomLogRecord::class.java,
            groupId = groupId
        )

        val subscriber1 = TestLogEventSubscriber(descriptor1)
        val subscriber2 = TestLogEventSubscriber(descriptor2)

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

        val blockchainScanner = createBlockchainScanner(testBlockchainClient, listOf(subscriber1, subscriber2))
        blockchainScanner.scan(once = true)

        assertThat(testLogRecordEventPublisher.publishedLogRecords).isEqualTo(
            mapOf(
                groupId to listOf(
                    LogRecordEvent(record = subscriber1.getReturnedRecords(block1, log11).single(), false),
                    LogRecordEvent(record = subscriber2.getReturnedRecords(block1, log11).single(), false),
                    LogRecordEvent(record = subscriber1.getReturnedRecords(block2, log21).single(), false),
                    LogRecordEvent(record = subscriber2.getReturnedRecords(block2, log21).single(), false)
                )
            )
        )
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
        val blockScanner = createBlockchainScanner(TestBlockchainClient(testBlockchainData), listOf(subscriber))
        blockScanner.scan(once = true)
        assertThat(findBlock(blocks[1].number)!!.copy(stats = null)).isEqualTo(blocks[1].toBlock(BlockStatus.SUCCESS))
        assertThat(findBlock(blocks[2].number)!!.copy(stats = null)).isEqualTo(blocks[2].toBlock(BlockStatus.SUCCESS))
        val confirmedLogs = listOf(
            LogRecordEvent(
                record = subscriber.getReturnedRecords(blocks[1], log11).single(),
                reverted = false
            ),
            LogRecordEvent(
                record = subscriber.getReturnedRecords(blocks[2], log21).single(),
                reverted = false
            ),
            LogRecordEvent(
                record = subscriber.getReturnedRecords(blocks[2], log22).single(),
                reverted = false
            ),
        )
        assertThat(testLogRecordEventPublisher.publishedLogRecords).isEqualTo(
            mapOf(descriptor.groupId to confirmedLogs)
        )

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
        val newBlockScanner = createBlockchainScanner(TestBlockchainClient(newTestBlockchainData), listOf(subscriber))
        newBlockScanner.scan(once = true)

        val revertedRecord22 = subscriber.getReturnedRecords(blocks[2], log22).single().revert()
        val revertedRecord21 = subscriber.getReturnedRecords(blocks[2], log21).single().revert()

        val newRecord21 = subscriber.getReturnedRecords(newBlock2, newLog21).single()
        val newRecord22 = subscriber.getReturnedRecords(newBlock2, newLog22).single()

        assertThat(testLogRecordEventPublisher.publishedLogRecords).isEqualTo(
            mapOf(
                descriptor.groupId to confirmedLogs + listOf(
                    LogRecordEvent(
                        record = revertedRecord22,
                        reverted = true
                    ),
                    LogRecordEvent(
                        record = revertedRecord21,
                        reverted = true
                    ),
                    LogRecordEvent(
                        record = newRecord21,
                        reverted = false
                    ),
                    LogRecordEvent(
                        record = newRecord22,
                        reverted = false
                    )
                )
            )
        )
        val savedLog21 = findLog(descriptor.collection, newRecord21.id)
        assertThat(savedLog21?.log?.reverted).isFalse
        val savedLog22 = findLog(descriptor.collection, newRecord22.id)
        assertThat(savedLog22?.log?.reverted).isFalse

        val savedRevertedLog22 = findLog(descriptor.collection, revertedRecord22.id)
        assertThat(savedRevertedLog22?.log?.reverted).isTrue
        val savedRevertedLog21 = findLog(descriptor.collection, revertedRecord21.id)
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
        val blockScanner = createBlockchainScanner(
            // Process only the blocks #0 and #1.
            TestBlockchainClient(testBlockchainData.copy(newBlocks = blocks.take(2))),
            listOf(subscriber)
        )
        blockScanner.scan(once = true)

        assertThat(findBlock(blocks[1].number)!!.copy(stats = null)).isEqualTo(blocks[1].toBlock(BlockStatus.SUCCESS))
        val confirmedLogs = listOf(
            LogRecordEvent(
                record = subscriber.getReturnedRecords(blocks[1], log11).single(),
                reverted = false
            ),
            LogRecordEvent(
                record = subscriber.getReturnedRecords(blocks[1], log12).single(),
                reverted = false
            )
        )
        assertThat(testLogRecordEventPublisher.publishedLogRecords).isEqualTo(
            mapOf(descriptor.groupId to confirmedLogs)
        )

        // Imitate the block #1 is PENDING.
        blockService.save(blocks[1].toBlock(BlockStatus.PENDING))
        val newBlockchainScanner = createBlockchainScanner(
            // Process all the blocks
            TestBlockchainClient(testBlockchainData),
            listOf(subscriber)
        )
        newBlockchainScanner.scan(once = true)

        // Block #1 was PENDING, so we, firstly, revert all possibly saved logs and then apply them again.
        assertThat(testLogRecordEventPublisher.publishedLogRecords).isEqualTo(
            mapOf(
                descriptor.groupId to confirmedLogs + listOf(
                    LogRecordEvent(
                        record = subscriber.getReturnedRecords(blocks[1], log12).single().revert(),
                        reverted = true
                    ),
                    LogRecordEvent(
                        record = subscriber.getReturnedRecords(blocks[1], log11).single().revert(),
                        reverted = true
                    ),
                    LogRecordEvent(
                        record = subscriber.getReturnedRecords(blocks[1], log11).single(),
                        reverted = false
                    ),
                    LogRecordEvent(
                        record = subscriber.getReturnedRecords(blocks[1], log12).single(),
                        reverted = false
                    ),
                    LogRecordEvent(
                        record = subscriber.getReturnedRecords(blocks[2], log21).single(),
                        reverted = false
                    )
                )
            )
        )
    }
}
