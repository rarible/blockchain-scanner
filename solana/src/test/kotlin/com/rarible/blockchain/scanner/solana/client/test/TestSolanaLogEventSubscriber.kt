package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import org.springframework.stereotype.Component

const val testRecordsCollection: String = "test-records"

@Component
class TestSolanaLogEventSubscriber : SolanaLogEventSubscriber {
    override fun getDescriptor(): SolanaDescriptor = object : SolanaDescriptor(
        programId = "testProgramId",
        id = "test",
        groupId = "testGroupId",
        entityType = TestSolanaLogRecord::class.java,
        collection = testRecordsCollection
    ) {}

    override suspend fun getEventRecords(
        block: SolanaBlockchainBlock,
        log: SolanaBlockchainLog
    ): List<SolanaLogRecord> = emptyList()
}
