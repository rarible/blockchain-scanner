package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import io.daonomic.rpc.domain.Word
import scalether.domain.response.Block

class EthereumBlockchainBlock(val ethBlock: Block<Word>) : BlockchainBlock {

    override val number = ethBlock.number().toLong()
    override val hash = ethBlock.hash().toString()
    override val parentHash = ethBlock.parentHash()?.toString()
    override val timestamp = ethBlock.timestamp().toLong()
}
