package com.rarible.blockchain.scanner.hedera.task

import com.rarible.blockchain.scanner.configuration.ReindexTaskProperties
import com.rarible.blockchain.scanner.configuration.TaskProperties
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.hedera.HederaBlockchainScannerManager
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainLog
import com.rarible.blockchain.scanner.hedera.configuration.HederaBlockchainScannerProperties
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage
import com.rarible.blockchain.scanner.hedera.model.HederaTransactionFilter
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.BlockScanPlanner
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class HederaBlockReindexTaskHandlerTest {

    private val reindexProperties = ReindexTaskProperties(enabled = true)
    private val taskProperties = TaskProperties(reindex = reindexProperties)
    private val scannerProperties = HederaBlockchainScannerProperties(task = taskProperties)
    private val reindexMonitor = mockk<ReindexMonitor>(relaxed = true)
    private val blockReindexer = mockk<BlockReindexer<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage>>()
    private val blockScanPlanner = mockk<BlockScanPlanner<HederaBlockchainBlock>>()
    private val logRecordEventPublisher = mockk<LogRecordEventPublisher>()

    private val mockManager = mockk<HederaBlockchainScannerManager> {
        every { properties } returns scannerProperties
        every { reindexMonitor } returns this@HederaBlockReindexTaskHandlerTest.reindexMonitor
        every { blockReindexer } returns this@HederaBlockReindexTaskHandlerTest.blockReindexer
        every { blockScanPlanner } returns this@HederaBlockReindexTaskHandlerTest.blockScanPlanner
        every { logRecordEventPublisher } returns this@HederaBlockReindexTaskHandlerTest.logRecordEventPublisher
    }

    @Test
    fun `should parse and create param correctly`() {
        val handler = HederaBlockReindexTaskHandler(mockManager)
        val jsonParam = """
            {
                "name": "test-reindex",
                "range": {
                    "from": 100,
                    "to": 200
                },
                "publishEvents": true,
                "transactionTypes": ["TOKENMINT", "TOKENBURN"]
            }
        """.trimIndent()

        val param = handler.getParam(jsonParam)
        assertThat(param).isNotNull
        assertThat(param.name).isEqualTo("test-reindex")
        assertThat(param.range.from).isEqualTo(100)
        assertThat(param.range.to).isEqualTo(200)
        assertThat(param.publishEvents).isTrue()
        assertThat(param.transactionTypes).containsOnly("TOKENMINT", "TOKENBURN")
    }

    @Test
    fun `should convert param to json correctly`() {
        val param = HederaReindexParam(
            name = "test-reindex",
            range = BlockRange(from = 100, to = 200, batchSize = 10),
            publishEvents = true,
            transactionTypes = listOf("TOKENMINT", "TOKENBURN")
        )

        val json = param.toJson()

        val expectedJson = """{"name":"test-reindex","range":{"from":100,"to":200,"batchSize":10},"publishEvents":true,"transactionTypes":["TOKENMINT","TOKENBURN"]}"""
        assertThat(json).isEqualTo(expectedJson)
    }

    @Test
    fun `should get filter correctly`() {
        val handler = HederaBlockReindexTaskHandler(mockManager)
        val param = HederaReindexParam(
            name = "test-reindex",
            range = BlockRange(from = 100, to = 200, batchSize = 100),
            publishEvents = true,
            transactionTypes = listOf("TOKENMINT", "TOKENBURN")
        )

        val filter = handler.getFilter(param)
        assertThat(filter).isInstanceOf(HederaSubscriberFilter::class.java)
    }

    @Test
    fun `filter should return all subscribers when transaction types is empty`() {
        val filter = HederaSubscriberFilter(emptyList())
        val subscribers = listOf(
            createMockSubscriber("TOKENMINT"),
            createMockSubscriber("TOKENBURN")
        )

        val result = filter.filter(subscribers)
        assertThat(result).hasSize(2)
        assertThat(result).contains(subscribers[0], subscribers[1])
    }

    @Test
    fun `filter should filter subscribers by transaction type`() {
        val filter = HederaSubscriberFilter(listOf("TOKENMINT"))
        val subscribers = listOf(
            createMockSubscriber("TOKENMINT"),
            createMockSubscriber("TOKENBURN")
        )

        val result = filter.filter(subscribers)
        assertThat(result).hasSize(1)
        assertThat(result[0]).isEqualTo(subscribers[0])
    }

    @Test
    fun `filter should handle multiple transaction types`() {
        val filter = HederaSubscriberFilter(listOf("TOKENMINT", "TOKENBURN"))
        val subscribers = listOf(
            createMockSubscriber("TOKENMINT"),
            createMockSubscriber("TOKENBURN"),
            createMockSubscriber("TOKENTRANSFER")
        )

        val result = filter.filter(subscribers)
        assertThat(result).hasSize(2)
        assertThat(result).contains(subscribers[0], subscribers[1])
    }

    @Test
    fun `filter should return empty list when no subscribers match`() {
        val filter = HederaSubscriberFilter(listOf("TOKENPAUSE"))
        val subscribers = listOf(
            createMockSubscriber("TOKENMINT"),
            createMockSubscriber("TOKENBURN")
        )

        val result = filter.filter(subscribers)
        assertThat(result).isEmpty()
    }

    @Test
    fun `filter should not match non-ByTransactionType filters`() {
        val filter = HederaSubscriberFilter(listOf("TOKENMINT"))
        val subscribers = listOf(
            createMockSubscriber("TOKENMINT"),
            createMockSubscriberWithEntityFilter("0.0.123")
        )

        val result = filter.filter(subscribers)
        assertThat(result).hasSize(1)
        assertThat(result[0]).isEqualTo(subscribers[0])
    }

    private fun createMockSubscriber(transactionType: String): LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage> {
        val descriptor = mockk<HederaDescriptor>()
        val subscriber = mockk<LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage>>()

        every { descriptor.filter } returns HederaTransactionFilter.ByTransactionType(transactionType)
        every { subscriber.getDescriptor() } returns descriptor

        return subscriber
    }

    private fun createMockSubscriberWithEntityFilter(entityId: String): LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage> {
        val descriptor = mockk<HederaDescriptor>()
        val subscriber = mockk<LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage>>()

        every { descriptor.filter } returns HederaTransactionFilter.ByEntityId(entityId)
        every { subscriber.getDescriptor() } returns descriptor

        return subscriber
    }
}
