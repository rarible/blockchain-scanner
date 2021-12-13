package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.data.annotation.Id

class SolanaBlock(
    @Id
    override val id: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long
) : Block