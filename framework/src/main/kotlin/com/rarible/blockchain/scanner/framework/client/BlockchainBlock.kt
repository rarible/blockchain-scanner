package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.framework.model.BlockMeta

/**
 * Simple wrapper for original Blockchain Block, provided by Blockchain Client.
 */
interface BlockchainBlock {

    val meta: BlockMeta

    val number: Long get() = meta.number
    val hash: String get() = meta.hash
    val parentHash: String? get() = meta.parentHash
    val timestamp: Long get() = meta.timestamp

}
