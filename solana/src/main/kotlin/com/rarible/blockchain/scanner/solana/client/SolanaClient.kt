package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.BlockHeader
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

class SolanaClient(
    url: String
) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {
    private val api = SolanaHttpRpcApi(url)

    override val newBlocks: Flow<SolanaBlockchainBlock>
        get() = flow {
            var lastSlot: Long = -1

            while (true) {
                val slot = getLatestSlot()

                if (slot != lastSlot) {
                    val block = api.getBlock(slot, TransactionDetails.None).toModel(slot)

                    lastSlot = slot
                    block?.let { emit(it) }
                }
            }
        }

    suspend fun getLatestSlot(): Long = api.getLatestSlot().toModel()

    override suspend fun getBlock(number: Long): SolanaBlockchainBlock? =
        api.getBlock(number, TransactionDetails.None).toModel(number)

    override fun getBlockLogs(
        descriptor: SolanaDescriptor,
        blocks: List<BlockHeader>,
        stable: Boolean
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> {
        // Normally, we have only one consequent range here.
        val ranges = BlockRanges.toRanges(blocks.map { it.number })
        return ranges.asFlow().map { getBlockLogs(descriptor, it) }.flattenConcat()
    }

    private fun getBlockLogs(
        descriptor: SolanaDescriptor,
        range: LongRange
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> = flow {
        for (slot in range) {
            val blockDto = api.getBlock(slot, TransactionDetails.Full)
            val solanaBlockchainBlock = blockDto.toModel(slot) ?: continue
            val solanaBlockchainLogs = blockDto.result!!.transactions.flatMap { transactionDto ->
                val hash = transactionDto.transaction.signatures.first()
                val blockHash = solanaBlockchainBlock.hash
                val events = transactionDto.toModel()

                events.filter { it.programId == descriptor.programId }
                    .map { event -> SolanaBlockchainLog(hash, blockHash, event) }
            }

            emit(FullBlock(solanaBlockchainBlock, solanaBlockchainLogs))
        }
    }
}
