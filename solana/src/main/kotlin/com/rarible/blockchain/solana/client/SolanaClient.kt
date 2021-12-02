package com.rarible.blockchain.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Component

@Component
class SolanaClient(
    url: String
) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {
    private val api = SolanaHttpRpcApi(url)

    override val newBlocks: Flow<SolanaBlockchainBlock>
        get() = api.getBlockFlow()

    override suspend fun getBlock(number: Long): SolanaBlockchainBlock? = api.getBlock(number)

    override suspend fun getLastBlockNumber(): Long = api.getLatestSlot()

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? =
        api.getTransaction(transactionHash)

    override fun getBlockEvents(
        descriptor: SolanaDescriptor,
        range: LongRange
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> {
        TODO("Not yet implemented")
    }
}
