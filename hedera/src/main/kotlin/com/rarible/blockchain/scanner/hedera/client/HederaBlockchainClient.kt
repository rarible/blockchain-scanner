package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Component

@Component
class HederaBlockchainClient : BlockchainClient<HederaBlockchainBlock, HederaBlockchainLog, HederaDescriptor> {

    override val newBlocks: Flow<HederaBlockchainBlock>
        get() = TODO("Not yet implemented")

    override suspend fun getBlock(number: Long): HederaBlockchainBlock? {
        TODO("Not yet implemented")
    }

    override suspend fun getBlocks(numbers: List<Long>): List<HederaBlockchainBlock> {
        TODO("Not yet implemented")
    }

    override suspend fun getFirstAvailableBlock(): HederaBlockchainBlock {
        TODO("Not yet implemented")
    }

    override suspend fun getLastBlockNumber(): Long {
        TODO("Not yet implemented")
    }

    override fun getBlockLogs(
        descriptor: HederaDescriptor,
        blocks: List<HederaBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<HederaBlockchainBlock, HederaBlockchainLog>> {
        TODO("Not yet implemented")
    }
}
