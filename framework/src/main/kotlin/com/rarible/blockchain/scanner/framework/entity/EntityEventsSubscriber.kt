package com.rarible.blockchain.scanner.framework.entity

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface EntityEventsSubscriber {
    suspend fun onEntityEvents(events: List<LogRecordEvent>)
}