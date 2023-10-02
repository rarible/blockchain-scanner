package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

@Deprecated(
    message = "Use from framework package", replaceWith = ReplaceWith(
        "com.rarible.blockchain.scanner.framework.entity.AbstractEntityEventListener"
    )
)
abstract class CompositeEntityEventListener(
    override val id: String,
    override val subscriberGroup: String,
    val entityEventsSubscribers: List<EntityEventsSubscriber>
) : EntityEventListener {

    override suspend fun onEntityEvents(events: List<LogRecordEvent>) {
        coroutineScope {
            entityEventsSubscribers.map {
                asyncWithTraceId(context = NonCancellable) { it.onEntityEvents(events) }
            }.awaitAll()
        }
    }
}
