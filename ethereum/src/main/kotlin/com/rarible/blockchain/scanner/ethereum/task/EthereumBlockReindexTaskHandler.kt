package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.ReindexParam
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.blockchain.scanner.task.BlockReindexTaskHandler
import io.daonomic.rpc.domain.Word
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class EthereumBlockReindexTaskHandler(
    @Qualifier("ethereumScannerManager")
    ethereumScannerManager: EthereumScannerManager,
    @Qualifier("reconciliationManager")
    reconciliationManager: EthereumScannerManager,
) : BlockReindexTaskHandler<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, TransactionRecord, EthereumDescriptor, EthereumLogRepository, EthereumReindexParam>(
    ethereumScannerManager
) {
    private val reconciliationReindexer = reconciliationManager.blockReindexer

    override fun getFilter(
        param: EthereumReindexParam
    ): SubscriberFilter<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor, EthereumLogRepository> {
        return EthereumSubscriberFilter(param.topics, param.addresses)
    }

    override fun getParam(param: String): EthereumReindexParam {
        return EthereumReindexParam.parse(param)
    }

    override fun getReindexer(param: EthereumReindexParam, defaultReindexer: EthereumBlockReindexer): EthereumBlockReindexer {
        return if (param.useReconciliationRpcNode) {
            reconciliationReindexer
        } else {
            super.getReindexer(param, defaultReindexer)
        }
    }
}

private typealias EthereumBlockReindexer = BlockReindexer<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor, EthereumLogRepository>

data class EthereumReindexParam(
    override val name: String? = null,
    override val range: BlockRange,
    override val publishEvents: Boolean = false,
    val topics: List<Word>,
    val addresses: List<Address>,
    val useReconciliationRpcNode: Boolean = false,
) : ReindexParam {

    companion object {
        fun parse(json: String) = ReindexParam.parse(json, EthereumReindexParam::class.java)
    }
}

class EthereumSubscriberFilter(
    private val topics: List<Word>,
    private val addresses: List<Address>,
) : SubscriberFilter<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor, EthereumLogRepository> {

    override fun filter(
        all: List<LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor, EthereumLogRepository>>
    ): List<LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor, EthereumLogRepository>> {

        val filteredSubscribers = all.filter {
            topics.isEmpty() || it.getDescriptor().ethTopic in topics
        }

        if (addresses.isEmpty()) {
            return filteredSubscribers
        }

        return filteredSubscribers.map {
            wrapSubscriberWithNewContracts(it as EthereumLogEventSubscriber, addresses)
        }
    }

    private fun wrapSubscriberWithNewContracts(
        subscriber: EthereumLogEventSubscriber,
        addresses: List<Address>
    ): EthereumLogEventSubscriber = object : EthereumLogEventSubscriber() {

        private val descriptor = subscriber.getDescriptor().copy(contracts = addresses)

        override suspend fun getEthereumEventRecords(
            block: EthereumBlockchainBlock,
            log: EthereumBlockchainLog
        ): List<EthereumLogRecord> = subscriber.getEthereumEventRecords(block, log)

        override fun getDescriptor(): EthereumDescriptor = descriptor
    }
}
