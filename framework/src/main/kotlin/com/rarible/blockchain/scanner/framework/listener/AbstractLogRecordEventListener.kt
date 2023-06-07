package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

abstract class AbstractLogRecordEventListener(
    override val id: String,
    override val groupId: String,
    private val subscribers: List<LogRecordEventSubscriber>,
) : LogRecordEventListener {

    override suspend fun onLogRecordEvents(events: List<LogRecordEvent>) {
        coroutineScope {
            subscribers.map {
                async { it.onLogRecordEvents(events) }
            }.awaitAll()
        }
    }
}
