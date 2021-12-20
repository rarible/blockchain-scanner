package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import io.daonomic.rpc.domain.Word
import scalether.domain.response.Block

data class EthereumBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long
) : BlockchainBlock {
    constructor(ethBlock: Block<*>) : this(
        number = ethBlock.number().toLong(),
        hash = ethBlock.hash().toString(),
        parentHash = ethBlock.parentHash()?.toString(),
        timestamp = ethBlock.timestamp().toLong()
    )
}
