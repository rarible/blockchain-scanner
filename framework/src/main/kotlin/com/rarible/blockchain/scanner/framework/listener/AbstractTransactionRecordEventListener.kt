package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

abstract class AbstractTransactionRecordEventListener(
    override val id: String,
    override val groupId: String,
    private val subscribers: List<TransactionRecordEventSubscriber>,
) : TransactionRecordEventListener {

    override suspend fun onTransactionRecordEvents(events: List<TransactionRecordEvent>) {
        coroutineScope {
            subscribers.map {
                asyncWithTraceId(context = NonCancellable) { it.onTransactionRecordEvents(events) }
            }.awaitAll()
        }
    }
}
