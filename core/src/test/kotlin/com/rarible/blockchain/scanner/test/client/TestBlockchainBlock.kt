package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.BlockMeta

data class TestBlockchainBlock(val testOriginalBlock: TestOriginalBlock) : BlockchainBlock {

    override val meta = BlockMeta(
        number = testOriginalBlock.number,
        hash = testOriginalBlock.hash,
        parentHash = testOriginalBlock.parentHash,
        timestamp = testOriginalBlock.timestamp
    )
}
