package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
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
interface LogService<L : Log, R : LogRecord<L, *>, D : Descriptor> {

    /**
     * Delete LogRecord from persistent storage.
     */
    suspend fun delete(descriptor: D, record: R): R

    /**
     * Delete multiple LogRecord-s from the persistent storage.
     */
    suspend fun delete(descriptor: D, records: List<R>): List<R>

    /**
     * Insert or update list of LogRecords to the persistent storage.
     */
    suspend fun save(descriptor: D, records: List<R>): List<R>

    /**
     * Returns log records that must be reverted when a new block is processed.
     */
    suspend fun prepareLogsToRevertOnNewBlock(descriptor: D, newBlock: FullBlock<*, *>): List<R>

    /**
     * Returns logs that must be reverted when a block is reverted.
     */
    suspend fun prepareLogsToRevertOnRevertedBlock(descriptor: D, revertedBlockHash: String): List<R>

}
