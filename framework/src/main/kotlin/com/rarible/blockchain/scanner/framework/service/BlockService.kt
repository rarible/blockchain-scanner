package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Block
import kotlinx.coroutines.flow.Flow

/**
 * Interface describes operations with persistent storage for Block records. Each Blockchain Scanner implementation
 * may have its own storage (Mongo/Postgres etc.).
 */
interface BlockService<B : Block> {

    /**
     * Return Block by number.
     */
    suspend fun getBlock(id: Long): B?

    /**
     * Return last stored Block (i.e. with max block number)
     */
    suspend fun getLastBlock(): B?

    /**
     * Update status of the Block.
     */
    suspend fun updateStatus(id: Long, status: Block.Status)

    /**
     * Insert or update Block record to the persistent storage.
     */
    suspend fun save(block: B)

    suspend fun remove(id: Long)

    fun findByStatus(status: Block.Status): Flow<B>

}