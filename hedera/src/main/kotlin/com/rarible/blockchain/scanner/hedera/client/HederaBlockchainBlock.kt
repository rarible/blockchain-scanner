package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import java.time.Instant

data class HederaBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    override val receivedTime: Instant = Instant.now(),
    val consensusTimestampFrom: String,
    val consensusTimestampTo: String,
) : BlockchainBlock {

    override fun withReceivedTime(value: Instant): BlockchainBlock {
        return copy(receivedTime = value)
    }
}
