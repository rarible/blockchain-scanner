package com.rarible.blockchain.scanner.framework.model

import java.time.Instant

/**
 * Basic data class for blockchain block data to be stored in persistent storage.
 */
interface Block {

    val id: Long
    val hash: String
    val parentHash: String?
    val timestamp: Long
    val status: Status

    enum class Status {
        PENDING,
        SUCCESS,
        ERROR
    }

    fun timestamp(): Instant = Instant.ofEpochSecond(timestamp)
}
