package com.rarible.blockchain.scanner.framework.entity

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

abstract class AbstractEntityEventListener(
    private val entityEventsSubscribers: List<EntityEventsSubscriber>
) : EntityEventListener {

    override suspend fun onEntityEvents(events: List<LogRecordEvent>) {
        coroutineScope {
            entityEventsSubscribers.map {
                async { it.onEntityEvents(events) }
            }.awaitAll()
        }
    }
}