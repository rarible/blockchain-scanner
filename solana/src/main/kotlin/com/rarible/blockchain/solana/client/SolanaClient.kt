package com.rarible.blockchain.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Component

@Component
class SolanaClient(
    url: String
) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {
    private val api = SolanaHttpRpcApi(url)

    override val newBlocks: Flow<SolanaBlockchainBlock>
        get() = api.getBlockFlow()

    suspend fun getLatestSlot(): Long = api.getLatestSlot()

    override suspend fun getBlock(number: Long): SolanaBlockchainBlock? = api.getBlock(number)

    override suspend fun getBlock(hash: String): SolanaBlockchainBlock? {
        TODO("Solana block by hash")
    }

    override fun getBlockLogs(
        descriptor: SolanaDescriptor,
        range: LongRange
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> {
        TODO("Not yet implemented")
    }
}
