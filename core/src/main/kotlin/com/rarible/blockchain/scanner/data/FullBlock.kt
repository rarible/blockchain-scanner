package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog

data class FullBlock<BB : BlockchainBlock, BL : BlockchainLog>(
    val block: BB,
    val logs: List<BL>
)