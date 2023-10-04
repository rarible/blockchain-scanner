package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import java.time.Instant

data class SolanaBlockchainBlock(
    val slot: Long,
    val parentSlot: Long,
    val logs: List<SolanaBlockchainLog>,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    override val receivedTime: Instant = Instant.now()
) : BlockchainBlock {

    override val number: Long = slot

    override fun withReceivedTime(value: Instant): BlockchainBlock {
        return copy(receivedTime = value)
    }

    override fun toString(): String = buildString {
        appendLine("Block #$slot:$hash (parent = #$parentSlot:$parentHash) at $timestamp")
        append(buildString {
            for (log in logs) {
                appendLine(log)
            }
        }.prependIndent(" "))
    }
}
