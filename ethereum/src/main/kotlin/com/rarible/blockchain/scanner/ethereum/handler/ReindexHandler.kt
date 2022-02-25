package com.rarible.blockchain.scanner.ethereum.handler

import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
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
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class ReindexHandler(
    private val allSubscribers: List<EthereumLogEventSubscriber>,
    private val blockService: BlockService,
    private val ethereumClient: EthereumBlockchainClient,
    private val logService: EthereumLogService,
    private val blockchainScannerProperties: BlockchainScannerProperties
) {
    private val retryableClient = RetryableBlockchainClient(
        original = ethereumClient,
        retryPolicy = blockchainScannerProperties.retryPolicy.client
    )

    suspend fun reindexFrom(
        fromBlock: Long,
        topics: List<Word> = emptyList(),
        addresses: List<Address> = emptyList()
    ): Flow<Long> {
        val lastBlock = blockService.getLastBlock()?.id ?: ethereumClient.newBlocks.first().number
        return reindexRange(LongRange(fromBlock, lastBlock), topics, addresses)
    }

    fun reindexRange(
        blockRange: LongRange,
        topics: List<Word> = emptyList(),
        addresses: List<Address> = emptyList()
    ): Flow<Long> {
        logger.info("Re-index blocks $blockRange")

        val filteredSubscribers = if (topics.isNotEmpty()) {
            allSubscribers.filter { subscriber -> subscriber.getDescriptor().ethTopic in topics }
        } else {
            allSubscribers
        }
        val wrappedSubscribers = if (addresses.isNotEmpty()) {
            filteredSubscribers.map { wrapSubscriberWithNewContracts(it, addresses) }
        } else {
            filteredSubscribers
        }

        val logHandlers = wrappedSubscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                logger.info(
                    "Reindex with subscribers of the group {}: {}",
                    groupId,
                    subscribers.joinToString { it.getDescriptor().id })
                LogHandler(
                    blockchainClient = retryableClient,
                    subscribers = subscribers,
                    logService = logService,
                    logRecordComparator = EthereumLogRecordComparator,
                    logRecordEventPublisher = reindexLogRecordEventPublisher
                )
            }

        val blockHandler = BlockHandler(
            blockClient = retryableClient,
            blockService = blockService,
            blockEventListeners = logHandlers,
            batchLoad = blockchainScannerProperties.scan.batchLoad
        )
        return blockHandler
            .batchIndexBlocks(fromId = blockRange.first, finishId = blockRange.last)
            .map {
                logger.info("Re-index finished up to block $it")
                it.id
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

    private val reindexLogRecordEventPublisher = object : LogRecordEventPublisher {
        override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent<*>>) {
            val blockNumber = (logRecordEvents.firstOrNull()?.record as? EthereumLogRecord)?.log?.blockNumber
            logger.info("Re-indexed log events for block $blockNumber: groupId=$groupId, size=${logRecordEvents.size}")
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReindexHandler::class.java)
    }
}
