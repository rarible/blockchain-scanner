package com.rarible.blockchain.scanner.solana.mapper

import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.model.SolanaBlock
import org.springframework.stereotype.Component

@Component
class SolanaBlockMapper : BlockMapper<SolanaBlockchainBlock, SolanaBlock> {
    override fun map(originalBlock: SolanaBlockchainBlock): SolanaBlock {
        return SolanaBlock(
            originalBlock.slot,
            originalBlock.hash,
            originalBlock.parentHash,
            originalBlock.timestamp
        )
    }
}