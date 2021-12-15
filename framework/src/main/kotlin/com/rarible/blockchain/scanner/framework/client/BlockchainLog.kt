package com.rarible.blockchain.scanner.framework.client

/**
 * Simple wrapper for original Blockchain Log, provided by Blockchain Client.
 */
interface BlockchainLog {

    /**
     * Hash of the transaction in which this log was created
     */
    val hash: String

    /**
     * Hash of the block to which the transaction belongs.
     */
    val blockHash: String?
}
