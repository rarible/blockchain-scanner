package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.core.entity.reducer.model.Entity
import com.rarible.core.entity.reducer.service.Reducer

abstract class RevertCompactEventsReducer<Id, Event : EthereumEntityEvent<Event>, E : Entity<Id, Event, E>> {

    suspend fun reduce(
        reducer: Reducer<Event, E>,
        entity: E,
        event: Event
    ): E {
        val compact = findCompact(event, entity.revertableEvents)
        return reducer.reduce(
            entity,
            if (compact != null) merge(event, compact) else event
        )
    }

    protected abstract fun merge(reverted: Event, compact: Event): Event

    private fun findCompact(event: Event, events: List<Event>): Event? {
        return events.firstOrNull { current ->
            current.compact &&
            current.log.status == EthereumBlockStatus.CONFIRMED &&
            current.compareTo(event) == 0
        }
    }
}