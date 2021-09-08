package com.rarible.blockchain.scanner.ethereum.mapper

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.stereotype.Component

@Component
class EthereumBlockMapper : BlockMapper<EthereumBlockchainBlock, EthereumBlock> {

    override fun map(originalBlock: EthereumBlockchainBlock, status: Block.Status): EthereumBlock {
        return EthereumBlock(
            id = originalBlock.number,
            hash = originalBlock.hash,
            parentHash = originalBlock.parentHash,
            timestamp = originalBlock.timestamp,
            status = status
        )
    }
}