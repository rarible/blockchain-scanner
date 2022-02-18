package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

abstract class SolanaDescriptor(
    val programId: String,
    override val id: String,
    override val groupId: String,
    override val entityType: Class<*>,
    val collection: String
) : Descriptor
