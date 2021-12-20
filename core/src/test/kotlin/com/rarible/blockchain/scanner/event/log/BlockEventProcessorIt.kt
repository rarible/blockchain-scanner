package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
class BlockEventProcessorIt : AbstractIntegrationTest() {

    @Test
    fun `process block events - single`() = runBlocking<Unit> {
        val descriptor = testDescriptor1()
        val subscriber = TestLogEventSubscriber(descriptor)

        val block = randomOriginalBlock()
        val log = randomOriginalLog(block.hash, descriptor.id)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block), listOf(log)))

        val blockEventProcessor = BlockEventProcessor(
            testBlockchainClient,
            listOf(subscriber),
            testLogService
        )

        val event = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val logEvents = blockEventProcessor.prepareBlockEvents(listOf(event))
        assertThat(logEvents).isEqualTo(
            listOf(
                LogEvent(
                    blockEvent = event,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber.getReturnedRecords(block, log),
                    logRecordsToRemove = emptyList()
                )
            )
        )
    }

    @Test
    fun `process block events - two blocks passed to two subscribers`() = runBlocking<Unit> {
        val descriptor = testDescriptor1()
        val subscriber1 = TestLogEventSubscriber(descriptor)
        val subscriber2 = TestLogEventSubscriber(descriptor)

        val block1 = randomOriginalBlock()
        val block2 = randomOriginalBlock()

        val log1 = randomOriginalLog(block1.hash, descriptor.id)
        val log2 = randomOriginalLog(block2.hash, descriptor.id)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block1, block2), listOf(log1, log2)))

        val blockEventProcessor = BlockEventProcessor(
            testBlockchainClient,
            listOf(subscriber1, subscriber2),
            testLogService
        )

        val event1 = NewBlockEvent(Source.BLOCKCHAIN, block1.number, block1.hash)
        val event2 = NewBlockEvent(Source.BLOCKCHAIN, block2.number, block2.hash)
        val logEvents = blockEventProcessor.prepareBlockEvents(listOf(event1, event2))
        assertThat(logEvents).isEqualTo(
            listOf(
                LogEvent(
                    blockEvent = event1,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber1.getReturnedRecords(block1, log1),
                    logRecordsToRemove = emptyList()
                ),
                LogEvent(
                    blockEvent = event2,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber1.getReturnedRecords(block2, log2),
                    logRecordsToRemove = emptyList()
                ),
                LogEvent(
                    blockEvent = event1,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber2.getReturnedRecords(block1, log1),
                    logRecordsToRemove = emptyList()
                ),
                LogEvent(
                    blockEvent = event2,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber2.getReturnedRecords(block2, log2),
                    logRecordsToRemove = emptyList()
                )
            )
        )
    }

}
