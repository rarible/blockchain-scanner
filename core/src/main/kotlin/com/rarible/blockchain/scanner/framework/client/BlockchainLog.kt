package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.LogMeta

/**
 * Simple wrapper for original Blockchain Log, provided by Blockchain Client.
 */
interface BlockchainLog {

    val meta: LogMeta

    val hash: String get() = meta.hash
    val blockHash: String get() = meta.blockHash
}