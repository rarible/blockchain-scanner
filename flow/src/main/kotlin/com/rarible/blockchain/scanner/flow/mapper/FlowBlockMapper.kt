package com.rarible.blockchain.scanner.flow.mapper

import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.model.FlowBlock
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.stereotype.Component

@Component
class FlowBlockMapper: BlockMapper<FlowBlockchainBlock, FlowBlock> {
    override fun map(originalBlock: FlowBlockchainBlock): FlowBlock {
        return FlowBlock(
            id = originalBlock.number,
            hash = originalBlock.hash,
            parentHash = originalBlock.parentHash!!,
            timestamp = originalBlock.timestamp,
            status = Block.Status.SUCCESS
        )
    }
}
