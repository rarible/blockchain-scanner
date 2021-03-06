package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.BlockMeta
import io.daonomic.rpc.domain.Word
import scalether.domain.response.Block

class EthereumBlockchainBlock(val ethBlock: Block<Word>) : BlockchainBlock {

    override val meta = BlockMeta(
        number = ethBlock.number().toLong(),
        hash = ethBlock.hash().toString(),
        parentHash = ethBlock.parentHash()?.toString(),
        timestamp = ethBlock.timestamp().toLong()
    )

}
