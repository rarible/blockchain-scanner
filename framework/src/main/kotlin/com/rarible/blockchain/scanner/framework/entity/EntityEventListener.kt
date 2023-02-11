package com.rarible.blockchain.scanner.framework.entity

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface EntityEventListener {
    val id: String

    val subscriberGroup: String

    suspend fun onEntityEvents(events: List<LogRecordEvent>)
}