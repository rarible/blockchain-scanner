package com.rarible.blockchain.scanner.ethereum.reduce.policy

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.core.entity.reducer.service.EventApplyPolicy

open class RevertEventApplyPolicy<T : EthereumEntityEvent<T>> : EventApplyPolicy<T> {
    override fun reduce(events: List<T>, event: T): List<T> {
        val confirmedEvents = events.filter {
            it.log.status == EthereumBlockStatus.CONFIRMED
        }
        require(confirmedEvents.isNotEmpty()) {
            "Can't revert from empty list (event=$event)"
        }
        require(event >= confirmedEvents.first()) {
            "Can't revert to old event (events=$events, event=$event)"
        }
        val confirmedEvent = findConfirmedEvent(confirmedEvents, event)
        return if (confirmedEvent != null) {
            // TODO: back after bug in blockchain scanner wiil be fixed
//            require(events.last() == confirmedEvent) {
//                "Event must revert from tail of list. Revert event: $event, event list=$events"
//            }
            events - confirmedEvent
        } else events
    }

    override fun wasApplied(events: List<T>, event: T): Boolean {
        return findConfirmedEvent(events, event) != null
    }

    private fun findConfirmedEvent(events: List<T>, event: T): T? {
        return events.firstOrNull { current ->
            current.log.status == EthereumBlockStatus.CONFIRMED && current.compareTo(event) == 0
        }
    }
}
