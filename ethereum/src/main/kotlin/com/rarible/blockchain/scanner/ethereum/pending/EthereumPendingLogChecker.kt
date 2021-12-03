package com.rarible.blockchain.scanner.ethereum.pending

import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumLogMapper
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.service.EthereumPendingLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toCollection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@FlowPreview
@Component
class EthereumPendingLogChecker(
    private val blockchainClient: EthereumClient,
    private val logMapper: EthereumLogMapper,
    private val logService: EthereumLogService,
    private val pendingLogService: EthereumPendingLogService,
    private val logEventPublisher: LogEventPublisher,
    subscribers: List<EthereumLogEventSubscriber>
) : PendingLogChecker {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogChecker::class.java)
    private val descriptors = subscribers.map { it.getDescriptor() }

    private val blockEventListeners = subscribers
        .groupBy { it.getDescriptor().groupId }
        .map {
            it.key to BlockEventListener(
                blockchainClient,
                it.value,
                logMapper,
                logService,
                logEventPublisher
            )
        }.associateBy({ it.first }, { it.second })

    override suspend fun checkPendingLogs() {
        val collections = descriptors.asFlow().flatMapConcat { descriptors ->
            pendingLogService.findPendingLogs(descriptors)
                .mapNotNull { processLog(descriptors, it) }
        }.toCollection(mutableListOf())

        val droppedLogs = collections.mapNotNull { it.first }
        val newBlocks = collections.mapNotNull { it.second }.distinctBy { it.hash }

        logEventPublisher.publish(droppedLogs)
        onNewBlocks(newBlocks)
    }

    private suspend fun onNewBlocks(newBlocks: List<BlockchainBlock>) {
        blockEventListeners.forEach { listener ->
            listener.value.onBlockEvents(newBlocks.map {
                NewBlockEvent(Source.PENDING, it.number, it.hash)
            })
        }
    }

    private suspend fun processLog(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord<*>,
    ): Pair<EthereumLogRecord<*>?, BlockchainBlock?>? {
        val tx = blockchainClient.getTransactionMeta(record.log!!.transactionHash)

        if (tx == null) {
            logger.info("Can't find transaction for record in blockchain, dropping it: [{}]", record)
            val updatedLog = pendingLogService.updateStatus(descriptor, record, Log.Status.DROPPED)
            return Pair(updatedLog, null)
        } else {
            val blockHash = tx.blockHash
            if (blockHash == null) {
                logger.info("Found pending transaction [{}] for log [{}], skipping", tx, record)
                return null
            }
            val block = blockchainClient.getBlock(blockHash)
            logger.info(
                "Found confirmed transaction [{}] for log [{}], updating entire block [{}:{}]",
                tx, record, block.number, blockHash
            )
            return Pair(null, block)
        }
    }
}
