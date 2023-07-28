package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

@Deprecated(message = "Use from framework package", replaceWith = ReplaceWith(
    "com.rarible.blockchain.scanner.framework.entity.EntityEventListener"
))
interface EntityEventListener {
    val id: String

    val subscriberGroup: String

    suspend fun onEntityEvents(events: List<LogRecordEvent>)
}
