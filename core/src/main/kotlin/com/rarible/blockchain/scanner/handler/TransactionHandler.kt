package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.subscriber.TransactionEventSubscriber
import com.rarible.blockchain.scanner.framework.util.addOut
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import org.slf4j.LoggerFactory

class TransactionHandler<BB : BlockchainBlock, R : TransactionRecord>(
    override val groupId: String,
    private val subscribers: List<TransactionEventSubscriber<BB, R>>,
    private val transactionRecordEventPublisher: TransactionRecordEventPublisher,
) : BlockEventListener<BB> {

    private val logger = LoggerFactory.getLogger(javaClass)

    override suspend fun process(events: List<BlockEvent<BB>>): BlockListenerResult {
        val transactionEvents = events.flatMap { blockEvent ->
            subscribers.map { subscriber ->
                val group = subscriber.getGroup()
                try {
                    val logEvents = when (blockEvent) {
                        is NewStableBlockEvent -> subscriber.getEventRecords(blockEvent.block).map {
                            TransactionRecordEvent(it, false, blockEvent.eventTimeMarks)
                        }

                        is NewUnstableBlockEvent -> subscriber.getEventRecords(blockEvent.block).map {
                            TransactionRecordEvent(it, false, blockEvent.eventTimeMarks)
                        }

                        is RevertedBlockEvent -> emptyList()
                    }
                    SubscriberResultOk(blockEvent.number, group, logEvents)
                } catch (e: Exception) {
                    logger.error("Failed to handle block {} by TX subscriber {}: ", blockEvent.number, group, e)
                    SubscriberResultFail(blockEvent.number, group, e.message ?: "Unknown error")
                }
            }
        }

        val failed = transactionEvents.filterIsInstance<SubscriberResultFail<List<TransactionRecordEvent>>>()
            .map { BlockError(it.blockNumber, groupId, it.errorMessage) }.groupBy { it.blockNumber }

        val records = transactionEvents.filterIsInstance<SubscriberResultOk<List<TransactionRecordEvent>>>()
            .flatMap { it.result }

        val result = events.map {
            BlockEventResult(
                blockNumber = it.number,
                errors = failed[it.number] ?: emptyList(),
                stats = BlockStats.empty()
            )
        }

        return BlockListenerResult(result) {
            if (transactionRecordEventPublisher.isEnabled()) {
                transactionRecordEventPublisher.publish(groupId, addOutMark(records))
            }
        }
    }

    private fun addOutMark(records: List<TransactionRecordEvent>): List<TransactionRecordEvent> {
        return records.map { it.copy(eventTimeMarks = it.eventTimeMarks.addOut()) }
    }
}
