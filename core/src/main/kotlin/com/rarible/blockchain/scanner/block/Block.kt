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
    val status: BlockStatus
)

enum class BlockStatus {
    PENDING,
    SUCCESS,
}

fun BlockchainBlock.toBlock(status: BlockStatus = BlockStatus.PENDING): Block =
    Block(number, hash, parentHash, timestamp, status)
