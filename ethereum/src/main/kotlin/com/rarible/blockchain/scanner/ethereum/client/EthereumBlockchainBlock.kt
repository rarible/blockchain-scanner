package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import scalether.domain.response.Block
import scalether.domain.response.Transaction

data class EthereumBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    val ethBlock: Block<Transaction>
) : BlockchainBlock {
    constructor(ethBlock: Block<Transaction>) : this(
        number = ethBlock.number().toLong(),
        hash = ethBlock.hash().toString(),
        parentHash = ethBlock.parentHash()?.toString(),
        timestamp = ethBlock.timestamp().toLong(),
        ethBlock
    )
}
