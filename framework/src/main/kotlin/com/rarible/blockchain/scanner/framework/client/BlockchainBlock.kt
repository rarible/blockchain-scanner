package com.rarible.blockchain.scanner.framework.client

/**
 * Simple wrapper for original Blockchain Block, provided by Blockchain Client.
 */
interface BlockchainBlock {

    val number: Long
    val hash: String
    val parentHash: String?
    val timestamp: Long

}
