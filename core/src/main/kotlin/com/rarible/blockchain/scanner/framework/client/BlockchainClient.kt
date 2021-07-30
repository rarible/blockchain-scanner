package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import kotlinx.coroutines.flow.Flow
import java.util.*

interface BlockchainClient<OB : BlockchainBlock, OL : BlockchainLog> {

    fun listenNewBlocks(): Flow<OB>

    suspend fun getBlock(id: Long): OB

    suspend fun getBlock(hash: String): OB

    suspend fun getLastBlockNumber(): Long

    suspend fun getBlockEvents(block: OB, descriptor: LogEventDescriptor): List<OL>

    fun getBlockEvents(descriptor: LogEventDescriptor, range: LongRange): Flow<BlockLogs<OL>>

    suspend fun getTransactionMeta(transactionHash: String): Optional<TransactionMeta>


}