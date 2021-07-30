package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import kotlinx.coroutines.flow.Flow
import java.util.*

interface BlockchainClient<BB : BlockchainBlock, BL : BlockchainLog> {

    fun listenNewBlocks(): Flow<BB>

    suspend fun getBlock(id: Long): BB

    suspend fun getBlock(hash: String): BB

    suspend fun getLastBlockNumber(): Long

    suspend fun getBlockEvents(block: BB, descriptor: LogEventDescriptor): List<BL>

    fun getBlockEvents(descriptor: LogEventDescriptor, range: LongRange): Flow<BlockLogs<BL>>

    suspend fun getTransactionMeta(transactionHash: String): Optional<TransactionMeta>


}