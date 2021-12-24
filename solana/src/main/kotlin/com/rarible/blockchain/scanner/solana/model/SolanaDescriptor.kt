package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.solana.client.SolanaBlockEvent

abstract class SolanaDescriptor(
    val programId: String,
    val collection: String,
    override val groupId: String
) : Descriptor {
    override val id = programId
    override val entityType = SolanaBlockEvent::class.java
}