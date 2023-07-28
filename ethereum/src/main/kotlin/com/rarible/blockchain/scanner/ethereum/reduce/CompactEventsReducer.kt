package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.core.entity.reducer.model.Entity
import com.rarible.core.entity.reducer.service.Reducer

abstract class CompactEventsReducer<Id, Event : EthereumEntityEvent<Event>, E : Entity<Id, Event, E>> : Reducer<Event, E> {

    override suspend fun reduce(entity: E, event: Event): E {
        val events = entity.revertableEvents
            .groupBy { it.log.blockNumber }
            .map { (_, blockEvents) ->
                blockEvents
                    .groupBy { it.javaClass }
                    .map { (_, typedEvents) ->
                        compact(typedEvents)
                    }
                    .flatten()
            }

        return entity.withRevertableEvents(events.flatten())
    }

    protected abstract fun compact(events: List<Event>): List<Event>
}
