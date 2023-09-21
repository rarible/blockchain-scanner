package com.rarible.blockchain.scanner.block

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import org.springframework.data.annotation.Id

/**
 * Basic data class for blockchain block data to be stored in persistent storage.
 */
data class Block(
    @Id
    val id: Long,

    val hash: String,
    val parentHash: String?,
    val timestamp: Long,
    val status: BlockStatus,
    val stats: BlockStats? = null
) {

    override fun toString(): String {
        return "[id=$id, hash=$hash, parent=$parentHash, ts=$timestamp, status=$status]"
    }
}

enum class BlockStatus {
    PENDING,
    SUCCESS,
}

fun BlockchainBlock.toBlock(status: BlockStatus = BlockStatus.PENDING, stats: BlockStats? = null): Block =
    Block(number, hash, parentHash, timestamp, status, stats)
