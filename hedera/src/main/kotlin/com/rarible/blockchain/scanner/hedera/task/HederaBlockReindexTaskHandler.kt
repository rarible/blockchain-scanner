package com.rarible.blockchain.scanner.hedera.task

import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.hedera.HederaBlockchainScannerManager
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainLog
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage
import com.rarible.blockchain.scanner.hedera.model.HederaTransactionFilter
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.ReindexParam
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.blockchain.scanner.task.BlockReindexTaskHandler
import org.springframework.stereotype.Component

@Component
class HederaBlockReindexTaskHandler(
    manager: HederaBlockchainScannerManager
) : BlockReindexTaskHandler<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, TransactionRecord, HederaDescriptor, HederaLogStorage, HederaReindexParam>(
    manager
) {

    override fun getFilter(
        param: HederaReindexParam
    ): SubscriberFilter<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage> {
        return HederaSubscriberFilter(param.transactionTypes)
    }

    override fun getParam(param: String): HederaReindexParam {
        return HederaReindexParam.parse(param)
    }
}

data class HederaReindexParam(
    override val name: String? = null,
    override val range: BlockRange,
    override val publishEvents: Boolean = false,
    val transactionTypes: List<String>
) : ReindexParam {

    companion object {
        fun parse(json: String) = ReindexParam.parse(json, HederaReindexParam::class.java)
    }

    override fun <T> copyWithRange(range: BlockRange) = this.copy(range = range) as T
}

class HederaSubscriberFilter(
    private val transactionTypes: List<String>
) : SubscriberFilter<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage> {

    override fun filter(
        all: List<LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage>>
    ): List<LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage>> {
        if (transactionTypes.isEmpty()) {
            return all
        }

        return all.filter { subscriber ->
            val descriptor = subscriber.getDescriptor()
            val filter = descriptor.filter
            if (filter is HederaTransactionFilter.ByTransactionType) {
                transactionTypes.contains(filter.transactionType)
            } else {
                false
            }
        }
    }
}
