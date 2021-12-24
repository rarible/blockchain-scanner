package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

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
        range: LongRange
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> = flow {
        for (slot in range) {
            val apiResponse = api.getBlock(slot, TransactionDetails.Full)
            val solanaBlockchainBlock = apiResponse.toModel(slot) ?: continue
            val blockDto = apiResponse.result!!

            val solanaBlockchainLogs = blockDto.transactions.flatMapIndexed { transactionIndex, transactionDto ->
                val result = mutableListOf<SolanaBlockchainLog>()
                val transaction = transactionDto.transaction
                val accountKeys = transaction.message.accountKeys
                val blockNumber = solanaBlockchainBlock.number
                val blockHash = solanaBlockchainBlock.hash
                val transactionHash = transactionDto.transaction.signatures.first()

                result += transaction.message.instructions.map {
                    it.toModel(
                        accountKeys,
                        blockNumber,
                        blockHash,
                        transactionHash,
                        transactionIndex,
                        Int.MIN_VALUE
                    )
                }

                result += transactionDto.meta.innerInstructions.flatMapIndexed { innerInstructionIndex, innerInstruction ->
                    innerInstruction.instructions.map { instruction ->
                        instruction.toModel(
                            accountKeys,
                            blockNumber,
                            blockHash,
                            transactionHash,
                            innerInstruction.index,
                            innerInstructionIndex
                        )
                    }
                }

                result.filter { it.instruction.programId == descriptor.programId }
            }

            emit(FullBlock(solanaBlockchainBlock, solanaBlockchainLogs))
        }
    }
}
