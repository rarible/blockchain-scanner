package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockDetails
import java.time.Instant

data class HederaBlockchainBlock(
    private val block: HederaBlockDetails,
    override val receivedTime: Instant = Instant.now()
) : BlockchainBlock {
    override val number: Long
        get() = block.number

    override val hash: String
        get() = block.hash

    override val parentHash: String
        get() = block.previousHash

    override val timestamp: Long
        get() = block.timestamp.from.toLong()

    override fun withReceivedTime(value: Instant): BlockchainBlock {
        return copy(receivedTime = value)
    }
}
