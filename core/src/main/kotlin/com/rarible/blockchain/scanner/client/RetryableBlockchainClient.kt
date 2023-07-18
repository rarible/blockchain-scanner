package com.rarible.blockchain.scanner.client

import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.flow.Flow

class RetryableBlockchainClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor>(
    private val original: BlockchainClient<BB, BL, D>,
    retryPolicy: ClientRetryPolicyProperties
) : BlockchainClient<BB, BL, D>, AbstractRetryableClient(retryPolicy) {

    override val newBlocks: Flow<BB> get() = original.newBlocks

    override suspend fun getFirstAvailableBlock(): BB {
        return wrapWithRetry("getBlock") {
            original.getFirstAvailableBlock()
        }
    }

    override suspend fun getLastBlockNumber(): Long {
        return wrapWithRetry("getLatestBlockNumber") {
            original.getLastBlockNumber()
        }
    }

    override suspend fun getBlocks(numbers: List<Long>): List<BB> {
        return wrapWithRetry("getBlocks", numbers) {
            original.getBlocks(numbers)
        }
    }

    override suspend fun getBlock(number: Long): BB? {
        return wrapWithRetry("getBlock", number) {
            original.getBlock(number)
        }
    }

    override fun getBlockLogs(descriptor: D, blocks: List<BB>, stable: Boolean): Flow<FullBlock<BB, BL>> {
        return original.getBlockLogs(descriptor, blocks, stable)
            // Full blocks produces too big messages in the logs
            .wrapWithRetry("getBlockLogs", blocks.map { it.toBlock() }, stable)
    }
}
