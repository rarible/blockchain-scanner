package com.rarible.blockchain.scanner.framework.client

/**
 * Simple wrapper for original Blockchain Log, provided by Blockchain Client.
 */
interface BlockchainLog {

    val hash: String
    val blockHash: String?
}
