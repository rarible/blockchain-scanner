package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import io.daonomic.rpc.domain.Word
import scalether.domain.response.Block

class EthereumBlockchainBlock(val ethBlock: Block<Word>) : BlockchainBlock {

    override val meta = BlockMeta(
        number = ethBlock.number().toLong(),
        hash = ethBlock.hash().hex(),
        parentHash = ethBlock.parentHash()?.hex(),
        timestamp = ethBlock.timestamp().toLong()
    )

}