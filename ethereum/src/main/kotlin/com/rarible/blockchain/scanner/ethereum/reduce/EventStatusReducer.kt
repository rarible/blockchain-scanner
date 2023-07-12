package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.core.entity.reducer.model.Entity
import com.rarible.core.entity.reducer.service.Reducer

abstract class EventStatusReducer<Id, Event : EthereumEntityEvent<Event>, E : Entity<Id, Event, E>>(
    private val forwardChainReducer: EntityChainReducer<Id, Event, E>,
    private val reversedChainReducer: RevertedEntityChainReducer<Id, Event, E>,
    private val revertCompactEventsReducer: RevertCompactEventsReducer<Id, Event, E>
) : Reducer<Event, E> {

    override suspend fun reduce(entity: E, event: Event): E {
        return when (event.log.status) {
            EthereumBlockStatus.CONFIRMED -> forwardChainReducer.reduce(entity, event)
            EthereumBlockStatus.REVERTED -> revertCompactEventsReducer.reduce(reversedChainReducer, entity, event)
            EthereumBlockStatus.PENDING,
            EthereumBlockStatus.INACTIVE,
            EthereumBlockStatus.DROPPED -> entity
        }
    }
}
