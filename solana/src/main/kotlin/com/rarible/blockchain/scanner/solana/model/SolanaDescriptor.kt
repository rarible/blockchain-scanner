package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

abstract class SolanaDescriptor(
    val programId: String,
    val collection: String,
    override val groupId: String,
    override val entityType: Class<*>
) : Descriptor {
    override val id = programId
}