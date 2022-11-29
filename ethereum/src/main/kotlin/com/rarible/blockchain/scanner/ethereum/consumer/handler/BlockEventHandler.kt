package com.rarible.blockchain.scanner.ethereum.consumer.handler

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecordEvent
import com.rarible.blockchain.scanner.ethereum.reduce.EntityEventListener
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.core.daemon.sequential.ConsumerBatchEventHandler
import scalether.domain.Address

class BlockEventHandler(
    private val entityEventListener: EntityEventListener,
    private val ignoreContracts: Set<Address>,
) : ConsumerBatchEventHandler<EthereumLogRecordEvent> {
    override suspend fun handle(event: List<EthereumLogRecordEvent>) {
        val filteredEvents = event.filter {
            ignoreContracts.contains(it.record.address).not()
        }
        entityEventListener.onEntityEvents(filteredEvents.map {
            LogRecordEvent(
                record = it.record,
                reverted = it.reverted
            )
        })
    }
}