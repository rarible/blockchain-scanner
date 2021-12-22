package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.flow.Flow

/**
 * Executor is an "entry point" of reconciliation (AKA "cold start" or "full re-sync") procedure.
 */
interface ReconciliationExecutor {

    /**
     * Start reconciliation for a group of descriptors starting from specified block number.
     *
     * @param groupId identifier of the descriptors group
     * @param from number of start block
     *
     * @return range of scanned blocks numbers
     */
    fun reconcile(groupId: String, from: Long, batchSize: Long): Flow<LongRange>

    /**
     * @return set of descriptors of all registered subscribers
     */
    fun getDescriptors(): List<Descriptor>

}
