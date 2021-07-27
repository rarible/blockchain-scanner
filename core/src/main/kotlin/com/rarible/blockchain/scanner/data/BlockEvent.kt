package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class BlockEvent<OB : BlockchainBlock>(
    val block: OB,
    val reverted: BlockchainBlock? = null
)