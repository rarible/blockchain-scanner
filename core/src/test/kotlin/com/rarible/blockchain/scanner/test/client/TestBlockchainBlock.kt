package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import java.time.Instant

data class TestBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    override val receivedTime: Instant,
    val testExtra: String
) : BlockchainBlock {

    override fun withReceivedTime(value: Instant): TestBlockchainBlock {
        return copy(receivedTime = value)
    }
}
