package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import scalether.domain.response.Block
import scalether.domain.response.Transaction
import java.time.Instant

data class EthereumBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    override val receivedTime: Instant,
    val ethBlock: Block<Transaction>
) : BlockchainBlock {

    constructor(ethBlock: Block<Transaction>) : this(ReceivedBlock(ethBlock))

    constructor(ethBlock: ReceivedBlock<Block<Transaction>>) : this(
        number = ethBlock.block.number().toLong(),
        hash = ethBlock.block.hash().toString(),
        parentHash = ethBlock.block.parentHash()?.toString(),
        timestamp = ethBlock.block.timestamp().toLong(),
        receivedTime = ethBlock.receivedTime,
        ethBlock.block
    )

    override fun getDatetime(): Instant = Instant.ofEpochSecond(timestamp)

    override fun withReceivedTime(value: Instant): BlockchainBlock {
        return this.copy(receivedTime = value)
    }
}
