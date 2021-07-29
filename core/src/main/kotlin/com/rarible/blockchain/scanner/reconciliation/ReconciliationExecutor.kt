package com.rarible.blockchain.scanner.reconciliation

import kotlinx.coroutines.flow.Flow

interface ReconciliationExecutor {

    suspend fun reconcile(topic: String?, from: Long): Flow<LongRange>

    fun getTopics(): Set<String>

}