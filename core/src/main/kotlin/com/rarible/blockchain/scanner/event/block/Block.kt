package com.rarible.blockchain.scanner.event.block

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