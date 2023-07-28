package com.rarible.blockchain.scanner.framework.listener

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent

interface TransactionRecordEventListener {
    val id: String

    val groupId: String

    suspend fun onTransactionRecordEvents(events: List<TransactionRecordEvent>)
}
