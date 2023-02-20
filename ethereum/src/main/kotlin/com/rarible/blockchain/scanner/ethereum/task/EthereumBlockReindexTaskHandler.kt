package com.rarible.blockchain.scanner.ethereum.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.ReindexParam
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.blockchain.scanner.task.BlockReindexTaskHandler
import io.daonomic.rpc.domain.Word
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class EthereumBlockReindexTaskHandler(
    manager: EthereumScannerManager
) : BlockReindexTaskHandler<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor, EthereumReindexParam>(
    manager
) {

    val mapper = ObjectMapper().registerModules().registerKotlinModule()

    override fun getFilter(
        param: EthereumReindexParam
    ): SubscriberFilter<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor> {
        return EthereumSubscriberFilter(param.topics, param.addresses)
    }

    override fun getParam(param: String): EthereumReindexParam {
        return mapper.readValue(param, EthereumReindexParam::class.java)
    }
}

data class EthereumReindexParam(
    override val name: String? = null,
    override val range: BlockRange,
    val topics: List<Word>,
    val addresses: List<Address>,
) : ReindexParam

class EthereumSubscriberFilter(
    private val topics: List<Word>,
    private val addresses: List<Address>,
) : SubscriberFilter<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor> {

    override fun filter(
        all: List<LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>>
    ): List<LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>> {

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
