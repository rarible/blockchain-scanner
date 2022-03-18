package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory

class SolanaClient(
    rpcUrls: List<String>,
    timeout: Long
) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {

    private val api = SolanaHttpRpcApi(
        urls = rpcUrls,
        timeoutMillis = timeout
    )

    override val newBlocks: Flow<SolanaBlockchainBlock>
        get() = flow {
            var latestSlot: Long = -1

            while (true) {
                val slot = getLatestSlot()

                if (slot != latestSlot) {
                    val block = api.getBlock(slot, TransactionDetails.Full).toModel(slot)

                    latestSlot = slot
                    block?.let { emit(it) }
                }
            }
        }

    suspend fun getLatestSlot(): Long = api.getLatestSlot().toModel()

    override suspend fun getBlock(number: Long): SolanaBlockchainBlock? =
        api.getBlock(number, TransactionDetails.Full).toModel(number)

    override fun getBlockLogs(
        descriptor: SolanaDescriptor,
        blocks: List<SolanaBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> {
        return blocks.asFlow()
            .map { block ->
                FullBlock(
                    block,
                    block.logs.filter { log -> log.instruction.programId == descriptor.programId }
                )
            }
    }

    override suspend fun getFirstAvailableBlock(): SolanaBlockchainBlock {
        val slot = api.getFirstAvailableBlock().toModel()
        val root = getBlock(slot)

        return if (root == null) {
            error("Can't find root block")
        } else {
            if (root.hash != root.parentHash) {
                logger.error("Root's parent hash != hash")
            }

            root
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SolanaClient::class.java)
    }
}
