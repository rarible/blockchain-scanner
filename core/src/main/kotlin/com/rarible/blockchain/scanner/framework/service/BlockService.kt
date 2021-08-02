package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Block
import kotlinx.coroutines.flow.Flow

//todo кажется, что BlockService не будет отличаться в разных блокчейнах и есть смысл сделать DefaultBlockService для какого стандартного блока
interface BlockService<B : Block> {

    fun findByStatus(status: Block.Status): Flow<B>

    suspend fun getLastBlock(): Long?

    suspend fun getBlockHash(id: Long): String?

    suspend fun updateBlockStatus(id: Long, status: Block.Status)

    suspend fun saveBlock(block: B)

    suspend fun findFirstByIdAsc(): B

    suspend fun findFirstByIdDesc(): B

}