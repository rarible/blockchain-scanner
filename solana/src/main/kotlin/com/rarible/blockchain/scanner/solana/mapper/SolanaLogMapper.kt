package com.rarible.blockchain.scanner.solana.mapper

import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import org.springframework.stereotype.Component

@Component
class SolanaLogMapper : LogMapper<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaLog> {
    override fun map(
        block: SolanaBlockchainBlock,
        log: SolanaBlockchainLog,
        index: Int,
        minorIndex: Int,
        descriptor: Descriptor
    ): SolanaLog {
        return SolanaLog(
            log.hash,
            status = Log.Status.CONFIRMED,
            block.number,
            log.event
        )
    }
}