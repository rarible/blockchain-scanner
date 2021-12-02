package com.rarible.blockchain.solana.client

import com.rarible.blockchain.scanner.framework.model.Descriptor

data class SolanaDescriptor(
    override val id: String,
    override val groupId: String
) : Descriptor