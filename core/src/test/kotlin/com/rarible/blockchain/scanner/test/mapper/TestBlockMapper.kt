package com.rarible.blockchain.scanner.test.mapper

import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.model.TestBlock

class TestBlockMapper : BlockMapper<TestBlockchainBlock, TestBlock> {

    override fun map(originalBlock: TestBlockchainBlock, status: Block.Status): TestBlock {
        return TestBlock(
            id = originalBlock.number,
            hash = originalBlock.hash,
            parentHash = originalBlock.parentHash,
            timestamp = originalBlock.timestamp,
            extra = originalBlock.testOriginalBlock.testExtra,
            status = status
        )
    }
}