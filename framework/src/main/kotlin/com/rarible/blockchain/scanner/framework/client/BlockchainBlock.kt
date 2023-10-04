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
    /**
     * Time when block was received by scanner
     */
    val receivedTime: Instant

    fun getDatetime(): Instant = Instant.ofEpochMilli(timestamp)

    fun withReceivedTime(value: Instant): BlockchainBlock
}
