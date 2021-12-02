package com.rarible.blockchain.scanner.ethereum.pending

import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumPendingLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventListener
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
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
    private val pendingLogService: EthereumPendingLogService,
    subscribers: List<EthereumLogEventSubscriber>,
    private val blockListener: BlockListener,
    private val logEventListeners: List<EthereumLogEventListener>
) : PendingLogChecker {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogChecker::class.java)
    private val descriptors = subscribers.map { it.getDescriptor() }

    override suspend fun checkPendingLogs() {
        val collections = descriptors.asFlow().flatMapConcat { descriptors ->
            pendingLogService.findPendingLogs(descriptors)
                .mapNotNull { processLog(descriptors, it) }
        }.toCollection(mutableListOf())


        val droppedLogs = collections.mapNotNull { it.first }
        val newBlocks = collections.mapNotNull { it.second }.distinctBy { it.hash }

        onDroppedLogs(droppedLogs)
        onNewBlocks(newBlocks)
    }

    private suspend fun onDroppedLogs(droppedLogs: List<EthereumLogRecord<*>>) {
        logEventListeners.forEach {
            try {
                it.onPendingLogsDropped(droppedLogs)
            } catch (ex: Throwable) {
                logger.error("Caught exception while onDroppedLogs logs of listener: {}", it.javaClass, ex)
            }
        }
    }

    private suspend fun onNewBlocks(newBlocks: List<BlockchainBlock>) {
        blockListener.onBlockEvents(newBlocks.map {
            NewBlockEvent(Source.PENDING, it.number, it.hash)
        })
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
