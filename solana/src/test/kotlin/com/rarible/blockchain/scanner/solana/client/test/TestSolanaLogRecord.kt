package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord

data class TestSolanaLogRecord(
    override val log: SolanaLog,
    private val recordKey: String
) : SolanaLogRecord() {
    override fun getKey(): String = recordKey
}
