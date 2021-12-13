package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Instant

@ObsoleteCoroutinesApi
@FlowPreview
@Configuration
@EnableAutoConfiguration
@EnableFlowBlockchainScanner
@ExperimentalCoroutinesApi
class TestConfig {

    @Bean
    fun allEventsSubscriber(): FlowLogEventSubscriber = object : FlowLogEventSubscriber {

        private val descriptor: FlowDescriptor = FlowDescriptor(
            id = "ExampleNFTDescriptor",
            groupId = "NFT",
            events = setOf("A.f8d6e0586b0a20c7.ExampleNFT.Mint"),
            collection = "test_history",
            entityType = TestFlowLogRecord::class.java
        )

        override fun getDescriptor(): FlowDescriptor = descriptor

        override suspend fun getEventRecords(
            block: FlowBlockchainBlock,
            log: FlowBlockchainLog,
            logMapper: LogMapper<FlowBlockchainBlock, FlowBlockchainLog, FlowLog>
        ): List<FlowLogRecord<*>> {
            if (!descriptor.events.contains(log.event.type)) {
                return emptyList()
            }
            val record = TestFlowLogRecord(
                log = FlowLog(
                    transactionHash = log.hash,
                    status = Log.Status.CONFIRMED,
                    timestamp = Instant.ofEpochSecond(block.timestamp),
                    blockHeight = block.number,
                    blockHash = block.hash,
                    eventIndex = log.event.eventIndex,
                    eventType = log.event.type
                ),
                data = log.event.payload.stringValue
            )
            return listOf(record)
        }
    }

}
