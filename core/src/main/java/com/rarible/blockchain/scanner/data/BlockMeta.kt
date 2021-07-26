package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class BlockMeta(
    override val number: Long,
    override val hash: String,
    override val parentHash: String,
    override val timestamp: Long
) : BlockchainBlock