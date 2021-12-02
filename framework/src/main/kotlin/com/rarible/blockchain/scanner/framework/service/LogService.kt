package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * Interface describes operations with persistent storage for Log Records. Each Blockchain Scanner implementation
 * may have its own storage (Mongo/Postgres etc.) and its own rules how to search/update LogRecord entities.
 * All operations should be executed in context of specific subscriber - its descriptor passed to all method calls.
 * For example, each subscriber has it own collection/table to store data. In such case descriptor can provide
 * name of the collection current operation should be executed to.
 */
interface LogService<L : Log<L>, R : LogRecord<L, *>, D : Descriptor> {

    /**
     * Delete LogRecord from persistent storage.
     */
    suspend fun delete(descriptor: D, record: R): R

    /**
     * Insert or update list of LogRecords to the persistent storage.
     */
    suspend fun save(descriptor: D, records: List<R>): List<R>

    /**
     * Operation performed before handling NewBlock. For example, here could be implemented
     * some cleanup of temporary LogEvents (like pending logs). As a result, list of updated events
     * should be emitted.
     *
     * @return updated/deleted LogRecords
     */
    suspend fun beforeHandleNewBlock(descriptor: D, blockHash: String): List<R>

    /**
     * Delete all LogRecords of specified block and with specified status.
     * Required to clean up LogRecords of reverted block.
     *
     * @return deleted LogRecords
     */
    suspend fun findAndDelete(descriptor: D, blockHash: String, status: Log.Status? = null): List<R>

}