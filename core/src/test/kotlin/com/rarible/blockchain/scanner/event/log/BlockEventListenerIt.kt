package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
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
internal class BlockEventListenerIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.id

    private val publishedRecords = arrayListOf<LogRecord<*, *>>()

    private val logEventPublisher = object : LogEventPublisher {
        override suspend fun publish(groupId: String, source: Source, logRecords: List<LogRecord<*, *>>) {
            publishedRecords += logRecords
        }

        override suspend fun publishDismissedLogs(
            descriptor: Descriptor,
            source: Source,
            logs: List<LogRecord<*, *>>
        ) = Unit
    }

    @Test
    fun `on block event - event processed`() = runBlocking<Unit> {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block1 = randomBlockchainBlock()
        val block2 = randomBlockchainBlock()

        val log1 = randomOriginalLog(block1.hash, topic)
        val log2 = randomOriginalLog(block2.hash, topic)

        saveBlock(block1.testOriginalBlock)
        saveBlock(block2.testOriginalBlock)

        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = listOf(block1.testOriginalBlock, block2.testOriginalBlock),
                logs = listOf(log1, log2)
            )
        )

        val blockEventListener = BlockEventListener(
            testBlockchainClient,
            arrayOf(subscriber).asList(),
            testLogService,
            testLogEventComparator,
            logEventPublisher
        )

        val event1 = NewBlockEvent(Source.BLOCKCHAIN, block1.number, block1.hash)
        val event2 = NewBlockEvent(Source.BLOCKCHAIN, block2.number, block2.hash)

        // Explicitly reversed order of events - to check the publishing order.
        blockEventListener.onBlockEvents(listOf(event2, event1))

        assertThat(publishedRecords).isEqualTo(
            listOf(
                subscriber.expectedRecords.getValue(block2.testOriginalBlock to log2).single(),
                subscriber.expectedRecords.getValue(block1.testOriginalBlock to log1).single(),
            )
        )
    }
}
