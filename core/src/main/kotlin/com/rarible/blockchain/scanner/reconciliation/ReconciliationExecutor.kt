package com.rarible.blockchain.scanner.reconciliation

import kotlinx.coroutines.flow.Flow

interface ReconciliationExecutor {

    fun reconcile(descriptorId: String?, from: Long): Flow<LongRange>

    fun getDescriptorIds(): Set<String>

}