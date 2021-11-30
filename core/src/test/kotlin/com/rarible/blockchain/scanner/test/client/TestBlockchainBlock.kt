package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class TestBlockchainBlock(
    val testOriginalBlock: TestOriginalBlock
) : BlockchainBlock {

    override val number = testOriginalBlock.number
    override val hash = testOriginalBlock.hash
    override val parentHash = testOriginalBlock.parentHash
    override val timestamp = testOriginalBlock.timestamp

}
