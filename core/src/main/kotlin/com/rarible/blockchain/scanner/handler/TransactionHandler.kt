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

class TransactionHandler<BB : BlockchainBlock, R : TransactionRecord>(
    private val groupId: String,
    private val subscribers: List<TransactionEventSubscriber<BB, R>>,
    private val transactionRecordEventPublisher: TransactionRecordEventPublisher,
) : BlockEventListener<BB> {

    override suspend fun process(events: List<BlockEvent<BB>>): Map<Long, BlockStats> {
        val transactionEvents = events.flatMap { blockEvent ->
            subscribers.flatMap { subscriber ->
                when (blockEvent) {
                    is NewStableBlockEvent -> subscriber.getEventRecords(blockEvent.block).map {
                        TransactionRecordEvent(it, false, blockEvent.eventTimeMarks)
                    }
                    is NewUnstableBlockEvent -> subscriber.getEventRecords(blockEvent.block).map {
                        TransactionRecordEvent(it, false, blockEvent.eventTimeMarks)
                    }
                    is RevertedBlockEvent -> emptyList()
                }
            }
        }
        if (transactionRecordEventPublisher.isEnabled()) {
            transactionRecordEventPublisher.publish(groupId, addOutMark(transactionEvents))
        }
        return emptyMap()
    }

    private fun addOutMark(records: List<TransactionRecordEvent>): List<TransactionRecordEvent> {
        return records.map { it.copy(eventTimeMarks = it.eventTimeMarks.addOut()) }
    }
}
