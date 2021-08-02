package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import kotlinx.coroutines.flow.Flow
import java.util.*

//todo можно добавить еще type param для LogEventDescriptor. у Flow там будут свои какие-то сущности. вроде, не сложно
interface BlockchainClient<BB : BlockchainBlock, BL : BlockchainLog> {

    fun listenNewBlocks(): Flow<BB>

    suspend fun getBlock(id: Long): BB

    suspend fun getBlock(hash: String): BB

    suspend fun getLastBlockNumber(): Long

    suspend fun getBlockEvents(block: BB, descriptor: LogEventDescriptor): List<BL>

    fun getBlockEvents(descriptor: LogEventDescriptor, range: LongRange): Flow<FullBlock<BB, BL>>

    suspend fun getTransactionMeta(transactionHash: String): Optional<TransactionMeta>


}