package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.assertOriginalLogAndLogEquals
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class BlockEventSubscriberIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.id

    @Test
    fun `on new block event`() = runBlocking {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block = randomOriginalBlock()
        val log = randomOriginalLog(block.hash, topic)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block), listOf(log)))

        val blockSubscriber = createBlockSubscriber(testBlockchainClient, subscriber)

        val event = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val logRecords = blockSubscriber.onNewBlockEvents(listOf(event)).flatMap { it.logRecords }

        // We're expecting single event from subscriber here
        assertThat(logRecords).hasSize(1)

        assertThat(logRecords[0].log.status).isEqualTo(Log.Status.CONFIRMED)
        assertThat(logRecords[0].log.visible).isTrue
        assertOriginalLogAndLogEquals(log, logRecords[0].log)
    }

    @Test
    fun `on reverted block event`() = runBlocking<Unit> {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block = randomOriginalBlock()
        val logRecord = randomTestLogRecord(topic, block.hash)

        val saved = testLogRepository.save(descriptor.collection, logRecord)

        val blockSubscriber = createBlockSubscriber(TestBlockchainClient(TestBlockchainData()), subscriber)

        val event = RevertedBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val logRecords = blockSubscriber.onRevertedBlockEvents(listOf(event)).flatMap { it.logRecords }

        // We're expecting here reverted log event with REVERTED status
        assertThat(logRecords).hasSize(1)

        val expectedRevertedLog = saved.withLog(saved.log.withStatus(Log.Status.REVERTED))

        assertThat(logRecords[0].log.status).isEqualTo(Log.Status.REVERTED)
        assertThat(logRecords[0].log.visible).isTrue
        assertThat(logRecords[0]).isEqualTo(expectedRevertedLog)
    }

    private fun createBlockSubscriber(
        testBlockchainClient: TestBlockchainClient,
        subscriber: TestLogEventSubscriber,
    ): BlockEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {
        return BlockEventSubscriber(
            testBlockchainClient,
            subscriber,
            testLogService
        )
    }
}
