package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class TestBlockchainBlock(val testOriginalBlock: TestOriginalBlock) : BlockchainBlock {

    override val meta = BlockMeta(
        number = testOriginalBlock.number,
        hash = testOriginalBlock.hash,
        parentHash = testOriginalBlock.parentHash,
        timestamp = testOriginalBlock.timestamp
    )
}