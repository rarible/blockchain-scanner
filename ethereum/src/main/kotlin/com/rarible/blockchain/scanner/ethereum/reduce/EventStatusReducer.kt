package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.core.entity.reducer.model.Entity
import com.rarible.core.entity.reducer.service.Reducer

abstract class EventStatusReducer<Id, Event : EthereumEntityEvent<Event>, E : Entity<Id, Event, E>>(
    private val forwardChainReducer: EntityChainReducer<Id, Event, E>,
    private val reversedChainReducer: EntityChainReducer<Id, Event, E>
) : Reducer<Event, E> {

    override suspend fun reduce(entity: E, event: Event): E {
        return when (event.log.status) {
            EthereumLogStatus.CONFIRMED -> forwardChainReducer.reduce(entity, event)
            EthereumLogStatus.REVERTED -> reversedChainReducer.reduce(entity, event)
            EthereumLogStatus.PENDING,
            EthereumLogStatus.INACTIVE,
            EthereumLogStatus.DROPPED -> entity
        }
    }
}
