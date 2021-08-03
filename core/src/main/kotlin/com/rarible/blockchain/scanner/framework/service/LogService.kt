package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import kotlinx.coroutines.flow.Flow

/**
 * Interface describes operations with persistent storage for Log Records. Each Blockchain Scanner implementation
 * may have its own storage (Mongo/Postgres etc.) and its own rules how to search/update LogRecord entities.
 * All operations should be executed in context of specific subscriber - its descriptor passed to all method calls.
 * For example, each subscriber has it own collection/table to store data. In such case descriptor can provide
 * name of the collection current operation should be executed to.
 */
interface LogService<L : Log, R : LogRecord<L, *>, D : Descriptor> {

    /**
     * Delete LogRecord from persistent storage.
     */
    suspend fun delete(descriptor: D, record: R): R

    /**
     * Insert or update LogRecord to the persistent storage.
     */
    suspend fun save(descriptor: D, record: R): R

    /**
     * Return all logs with status [PENDING][com.rarible.blockchain.scanner.framework.model.Log.Status.PENDING] for
     * specified subscriber's descriptor.
     *
     */
    fun findPendingLogs(descriptor: D): Flow<R>

    /**
     * Set [PENDING][com.rarible.blockchain.scanner.framework.model.Log.Status.REVERTED]
     * for all Log Records of specified block.
     *
     * @return updated LogRecords
     */
    fun findAndRevert(descriptor: D, blockHash: String): Flow<R>

    /**
     * Delete all LogRecords of specified block and with specified status.
     *
     * @return deleted LogRecords
     */
    fun findAndDelete(descriptor: D, blockHash: String, status: Log.Status? = null): Flow<R>

    /**
     * Update status of single LogRecord
     *
     * @return updated LogRecord
     */
    suspend fun updateStatus(descriptor: D, record: R, status: Log.Status): R


}