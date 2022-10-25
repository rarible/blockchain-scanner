package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.core.apm.withTransaction
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

abstract class CompositeEntityEventListener(
    override val id: String,
    override val subscriberGroup: String,
    val entityEventsSubscribers: List<EntityEventsSubscriber>
) : EntityEventListener {

    override suspend fun onEntityEvents(events: List<LogRecordEvent>) {
        withTransaction("onEntityEvents", labels = listOf("size" to events.size)) {
            coroutineScope {
                entityEventsSubscribers.map {
                    async { it.onEntityEvents(events) }
                }.awaitAll()
            }
        }
    }
}