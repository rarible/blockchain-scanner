package com.rarible.blockchain.scanner.ethereum.handler

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogRecordComparator
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.handler.ReindexBlockHandler
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class ReindexHandler(
    private val subscribers: List<EthereumLogEventSubscriber>,
    ethereumClient: EthereumBlockchainClient,
    logService: EthereumLogService,
    properties: BlockchainScannerProperties
) {
    private val reindexBlockHandler = ReindexBlockHandler(
        ethereumClient,
        logService,
        EthereumLogRecordComparator,
        properties
    )

    fun reindex(
        block: Long,
        topics: List<Word> = emptyList(),
        addresses: List<Address> = emptyList()
    ): Flow<Long> {
        return reindex(LongRange(block, block), topics, addresses)
    }

    fun reindex(
        blockRange: LongRange,
        topics: List<Word> = emptyList(),
        addresses: List<Address> = emptyList()
    ): Flow<Long> {
        val filteredSubscribers = if (topics.isNotEmpty()) {
            subscribers.filter { subscriber -> subscriber.getDescriptor().ethTopic in topics }
        } else {
            subscribers
        }
        val wrappedSubscribers = if (addresses.isEmpty()) {
            filteredSubscribers.map { wrapSubscriber(it, addresses) }
        } else {
            filteredSubscribers
        }
        return reindexBlockHandler.reindexBlocksLogs(
            blockRange,
            wrappedSubscribers,
            reindexLogRecordEventPublisher
        ).map {
            logger.info("Reindex to block: number=${it.number}, hash=${it.hash}")
            it.number
        }
    }

    private fun wrapSubscriber(subscriber: EthereumLogEventSubscriber, addresses: List<Address>): EthereumLogEventSubscriber {
        return object : EthereumLogEventSubscriber() {
            private val delegate = subscriber

            private val descriptor = run {
                val delegateDescriptor = delegate.getDescriptor()

                EthereumDescriptor(
                    ethTopic = delegateDescriptor.ethTopic,
                    groupId = delegateDescriptor.groupId,
                    collection = delegateDescriptor.collection,
                    contracts = addresses,
                    entityType = delegateDescriptor.entityType,
                    id = delegateDescriptor.id
                )
            }

            override suspend fun getEthereumEventRecords(
                block: EthereumBlockchainBlock,
                log: EthereumBlockchainLog
            ): List<EthereumLogRecord> {
                return subscriber.getEthereumEventRecords(block, log)
            }

            override fun getDescriptor(): EthereumDescriptor {
                return descriptor
            }
        }
    }

    private val reindexLogRecordEventPublisher = object : LogRecordEventPublisher {
        override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent<*>>) {
            val blockNumber =  (logRecordEvents.firstOrNull()?.record as? EthereumLogRecord)?.log?.blockNumber
            logger.info("Reindex events for block $blockNumber: groupId=$groupId, size=${logRecordEvents.size}")
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReindexHandler::class.java)
    }
}