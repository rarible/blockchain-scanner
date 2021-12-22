package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

class SolanaClient(url: String) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {
    private val api = SolanaHttpRpcApi(url)

    override val newBlocks: Flow<SolanaBlockchainBlock>
        get() = flow {
            var lastSlot: Long = -1

            while (true) {
                val slot = getLatestSlot()

                if (slot != lastSlot) {
                    val block = api.getBlock(slot, TransactionDetails.None)

                    lastSlot = slot
                    block?.let { emit(it.toModel(slot)) }
                }
                delay(SolanaHttpRpcApi.POLLING_DELAY)
            }
        }

    suspend fun getLatestSlot(): Long = api.getLatestSlot()

    override suspend fun getBlock(number: Long): SolanaBlockchainBlock? =
        api.getBlock(number, TransactionDetails.None)?.toModel(number)

    override fun getBlockLogs(
        descriptor: SolanaDescriptor,
        range: LongRange
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> = flow {
        range.forEach { slot ->
            val blockDto = api.getBlock(slot, TransactionDetails.Full)

            if (blockDto != null) {
                val solanaBlockchainBlock = blockDto.toModel(slot)
                val solanaBlockchainLogs = blockDto.transactions.flatMap { transactionDto ->
                    val hash = transactionDto.transaction.signatures.first()
                    val blockHash = solanaBlockchainBlock.hash
                    val events = transactionDto.toModel()

                    events
                        .filter { it.programId == descriptor.id }
                        .map { event -> SolanaBlockchainLog(hash, blockHash, event) }
                }

                emit(FullBlock(solanaBlockchainBlock, solanaBlockchainLogs))
            }
        }
    }
}
