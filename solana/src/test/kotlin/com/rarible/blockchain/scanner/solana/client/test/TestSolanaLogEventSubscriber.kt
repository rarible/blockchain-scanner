package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaInstructionFilter
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.stereotype.Component

const val testRecordsCollection: String = "test-records"

@Component
class TestSolanaLogEventSubscriber(
    mongo: ReactiveMongoTemplate,
) : SolanaLogEventSubscriber {
    private val testLogStorage = TestSolanaLogStorage(mongo, testRecordsCollection)

    override fun getDescriptor(): SolanaDescriptor = object : SolanaDescriptor(
        filter = SolanaInstructionFilter.ByProgramId("testProgramId"),
        id = "test",
        groupId = "testGroupId",
        storage = testLogStorage,
    ) {}

    override suspend fun getEventRecords(
        block: SolanaBlockchainBlock,
        log: SolanaBlockchainLog
    ): List<SolanaLogRecord> = emptyList()
}
