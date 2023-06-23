package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent

/**
 * Client side subscriber for TransactionRecordEvent
 */
interface TransactionRecordEventSubscriber {
    suspend fun onTransactionRecordEvents(events: List<TransactionRecordEvent>)
}