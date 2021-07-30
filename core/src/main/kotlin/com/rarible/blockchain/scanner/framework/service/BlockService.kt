package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Block
import kotlinx.coroutines.flow.Flow

interface BlockService<B : Block> {

    fun findByStatus(status: Block.Status): Flow<B>

    suspend fun getLastBlock(): Long?

    suspend fun getBlockHash(id: Long): String?

    suspend fun updateBlockStatus(id: Long, status: Block.Status)

    suspend fun saveBlock(block: B)

    suspend fun findFirstByIdAsc(): B

    suspend fun findFirstByIdDesc(): B

}