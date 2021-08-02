package com.rarible.blockchain.scanner.reconciliation

import kotlinx.coroutines.flow.Flow

interface ReconciliationExecutor {

    //todo нужно убрать suspend функции, который Flow возвращают (обсуждали как раз как это сделать)
    suspend fun reconcile(topic: String?, from: Long): Flow<LongRange>

    fun getTopics(): Set<String>

}