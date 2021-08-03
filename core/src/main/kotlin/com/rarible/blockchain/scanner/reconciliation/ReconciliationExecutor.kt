package com.rarible.blockchain.scanner.reconciliation

import kotlinx.coroutines.flow.Flow

/**
 * Executor is an "entry point" of reconciliation (AKA "cold start" or "full re-sync") procedure.
 */
interface ReconciliationExecutor {

    /**
     * Start reconciliation for specific descriptor from specified block number.
     *
     * @param descriptorId identifier of subscriber's descriptor requires reconciliation
     * @param from number of start block
     *
     * @return range of scanned blocks numbers
     */
    fun reconcile(descriptorId: String?, from: Long): Flow<LongRange>

    /**
     * @return set of descriptors of all registered subscribers
     */
    fun getDescriptorIds(): Set<String>

}