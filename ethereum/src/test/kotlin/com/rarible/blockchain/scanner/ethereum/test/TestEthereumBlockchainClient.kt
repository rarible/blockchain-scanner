package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.data.FullBlock
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Wrapper for ethereum client that ignores all blocks before the given threshold and allows delaying block processing.
 */
class TestEthereumBlockchainClient(
    private val delegate: EthereumBlockchainClient
) : EthereumBlockchainClient {

    val blocksDelayLock = ReentrantLock()

    var startingBlock: Long = 0L

    override val newBlocks: Flow<EthereumBlockchainBlock>
        get() = delegate.newBlocks.onEach {
            // Check if the block processing is delayed.
            blocksDelayLock.withLock {
            }
        }

    override suspend fun getLastBlockNumber(): Long {
        return delegate.getLastBlockNumber()
    }

    override suspend fun getBlocks(numbers: List<Long>): List<EthereumBlockchainBlock> =
        numbers.mapNotNull { getBlock(it) }

    override suspend fun getBlock(number: Long): EthereumBlockchainBlock? {
        val zeroBlock = delegate.getBlock(0L)
        if (number == 0L) {
            return zeroBlock
        }
        if (number == startingBlock) {
            val block = delegate.getBlock(number)
            return block?.copy(parentHash = zeroBlock?.hash)
        }
        if (number < startingBlock) {
            return null
        }
        return delegate.getBlock(number)
    }

    override suspend fun getFirstAvailableBlock(): EthereumBlockchainBlock =
        getBlock(0) ?: error("Can't find root block")

    override fun getBlockLogs(
        descriptor: EthereumDescriptor,
        blocks: List<EthereumBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> =
        delegate.getBlockLogs(descriptor, blocks, stable)
}
