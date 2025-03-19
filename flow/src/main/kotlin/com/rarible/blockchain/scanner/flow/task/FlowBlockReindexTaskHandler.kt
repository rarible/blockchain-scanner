package com.rarible.blockchain.scanner.flow.task

import com.rarible.blockchain.scanner.flow.FlowBlockchainScannerManager
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogStorage
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
) : BlockReindexTaskHandler<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, TransactionRecord, FlowDescriptor, FlowLogStorage, FlowReindexParam>(
    manager
) {

    override fun getFilter(
        param: FlowReindexParam
    ): SubscriberFilter<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor, FlowLogStorage> {
        return FlowSubscriberFilter(param.addresses)
    }

    override fun getParam(param: String): FlowReindexParam {
        return FlowReindexParam.parse(param)
    }
}

data class FlowReindexParam(
    override val name: String? = null,
    override val range: BlockRange,
    override val publishEvents: Boolean = true,
    val addresses: Set<String> = emptySet()
) : ReindexParam {

    companion object {
        fun parse(json: String) = ReindexParam.parse(json, FlowReindexParam::class.java)
    }

    override fun <T> copyWithRange(range: BlockRange) = this.copy(range = range) as T
}

class FlowSubscriberFilter(
    private val addresses: Set<String>
) : SubscriberFilter<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor, FlowLogStorage> {

    override fun filter(
        all: List<LogEventSubscriber<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor, FlowLogStorage>>
    ): List<LogEventSubscriber<FlowBlockchainBlock, FlowBlockchainLog, FlowLogRecord, FlowDescriptor, FlowLogStorage>> {
        if (addresses.isEmpty()) {
            return all
        }
        return all.filter { addresses.contains(it.getDescriptor().address) }
    }
}
