package com.rarible.blockchain.scanner.framework.client

import java.time.Instant

/**
 * Simple wrapper for original Blockchain Block, provided by Blockchain Client.
 */
interface BlockchainBlock {

    val number: Long
    val hash: String
    val parentHash: String?
    val timestamp: Long

    fun getDatetime(): Instant = Instant.ofEpochMilli(timestamp)

}
