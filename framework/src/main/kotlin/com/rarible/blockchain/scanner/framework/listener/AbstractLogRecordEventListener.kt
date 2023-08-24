package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.NonCancellable
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
                asyncWithTraceId(context = NonCancellable) { it.onLogRecordEvents(events) }
            }.awaitAll()
        }
    }
}
