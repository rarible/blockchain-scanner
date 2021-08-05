package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Block
import kotlinx.coroutines.flow.Flow

//todo кажется, что BlockService не будет отличаться в разных блокчейнах и есть смысл сделать DefaultBlockService для какого стандартного блока
/**
 * Interface describes operations with persistent storage for Block records. Each Blockchain Scanner implementation
 * may have its own storage (Mongo/Postgres etc.).
 */
interface BlockService<B : Block> {

    fun findByStatus(status: Block.Status): Flow<B>

    /**
     * Return last (i.e.MAX) stored Block number.
     */
    suspend fun getLastBlockNumber(): Long?

    /**
     * Return Block by number.
     */
    suspend fun getBlock(id: Long): B?

    /**
     * Update status of the Block.
     */
    suspend fun updateStatus(id: Long, status: Block.Status)

    /**
     * Insert or update Block record to the persistent storage.
     */
    suspend fun save(block: B)

}