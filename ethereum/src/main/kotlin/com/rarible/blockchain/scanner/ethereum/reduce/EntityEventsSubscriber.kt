package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

@Deprecated(message = "Use from framework package", replaceWith = ReplaceWith(
    "com.rarible.blockchain.scanner.framework.entity.EntityEventsSubscriber"
))
interface EntityEventsSubscriber {
    suspend fun onEntityEvents(events: List<LogRecordEvent>)
}