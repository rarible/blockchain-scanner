package com.rarible.blockchain.scanner.flow.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.flow.FlowBlockchainScannerManager
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.ReindexParam
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.blockchain.scanner.task.BlockReindexTaskHandler
import org.springframework.stereotype.Component

@Component
class FlowBlockReindexTaskHandler(
    manager: FlowBlockchainScannerManager
) : BlockReindexTaskHandler<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, TransactionRecord, FlowDescriptor, FlowReindexParam>(
    manager
) {

    val mapper = ObjectMapper().registerModules().registerKotlinModule()

    override fun getFilter(
        param: FlowReindexParam
    ): SubscriberFilter<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor> {
        return FlowSubscriberFilter(param.addresses)
    }

    override fun getParam(param: String): FlowReindexParam {
        return mapper.readValue(param, FlowReindexParam::class.java)
    }
}

data class FlowReindexParam(
    override val name: String? = null,
    override val range: BlockRange,
    override val publishEvents: Boolean = true,
    val addresses: Set<String> = emptySet()
) : ReindexParam

class FlowSubscriberFilter(
    private val addresses: Set<String>
) : SubscriberFilter<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor> {

    override fun filter(
        all: List<LogEventSubscriber<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor>>
    ): List<LogEventSubscriber<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor>> {
        if (addresses.isEmpty()) {
            return all
        }
        return all.filter { addresses.contains(it.getDescriptor().address) }
    }
}
