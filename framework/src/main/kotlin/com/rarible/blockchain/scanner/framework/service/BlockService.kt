package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Block

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
     * Insert or update Block record to the persistent storage.
     */
    suspend fun save(block: B)

    suspend fun remove(id: Long)

}