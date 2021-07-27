package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import io.daonomic.rpc.domain.Word
import scalether.domain.response.Block

class EthereumBlockchainBlock(val ethBlock: Block<Word>) : BlockchainBlock {

    override val number: Long
        get() = ethBlock.number().toLong()
    override val hash: String
        get() = ethBlock.hash().toString()
    override val parentHash: String
        get() = ethBlock.parentHash().toString()
    override val timestamp: Long
        get() = ethBlock.timestamp().toLong()
}