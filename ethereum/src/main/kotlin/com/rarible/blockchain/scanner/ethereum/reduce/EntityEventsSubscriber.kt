package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface EntityEventsSubscriber {
    suspend fun onEntityEvents(events: List<LogRecordEvent>)
}