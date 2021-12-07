package com.rarible.blockchain.scanner.ethereum.pending

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.service.EthereumPendingLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import java.util.*

@FlowPreview
class EthereumPendingLogChecker(
    private val blockchainClient: EthereumBlockchainClient,
    private val pendingLogService: EthereumPendingLogService,
    private val logEventPublisher: LogEventPublisher,
    private val blockEventListeners: Map<String, BlockListener>,
    subscribers: List<EthereumLogEventSubscriber>
) {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogChecker::class.java)
    private val descriptors = subscribers.map { it.getDescriptor() }

    suspend fun checkPendingLogs() {
        val newBlocks = TreeSet<NewBlockEvent> { b1, b2 -> b1.number.compareTo(b2.number) }
        val droppedRecords = LinkedHashMap<Descriptor, MutableList<EthereumLogRecord<*>>>()

        descriptors.forEach { descriptor ->
            pendingLogService.findPendingLogs(descriptor)
                .mapNotNull { processLog(descriptor, it) }
                .toList()
                .forEach { pair ->
                    val record = pair.first
                    val block = pair.second
                    block?.let { newBlocks.add(NewBlockEvent(Source.PENDING, it.number, it.hash)) }
                    record?.let { droppedRecords.computeIfAbsent(descriptor) { mutableListOf() }.add(record) }
                }
        }
        drop(droppedRecords)
        reindexBlocks(newBlocks.toList())
    }

    private suspend fun reindexBlocks(blocks: List<NewBlockEvent>) {
        if (blocks.isEmpty()) {
            logger.info("Blocks with pending logs not found, nothing to reindex")
            return
        }
        logger.info("Found blocks with pending logs, should be reindexed: {}", blocks)
        coroutineScope {
            blockEventListeners.map { listener ->
                async {
                    listener.value.onBlockEvents(blocks)
                }
            }.awaitAll()
        }
    }

    private suspend fun drop(droppedRecords: Map<Descriptor, List<EthereumLogRecord<*>>>) {
        if (droppedRecords.isEmpty()) {
            logger.info("Pending logs not found, nothing to publish")
            return
        }
        logger.info("Dropped pending logs for {} descriptors, publishing events now", droppedRecords.size)
        droppedRecords.forEach {
            logger.info("Publishing {} events for descriptor {}", it.value.size, it.key.id)
            logEventPublisher.publish(it.key, Source.PENDING, it.value)
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
