package com.rarible.blockchain.scanner.ethereum.mapper

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper

class EthereumBlockMapper : BlockMapper<EthereumBlockchainBlock, EthereumBlock> {

    override fun map(originalBlock: EthereumBlockchainBlock): EthereumBlock {
        return EthereumBlock(
            id = originalBlock.number,
            hash = originalBlock.hash,
            timestamp = originalBlock.timestamp
        )
    }
}