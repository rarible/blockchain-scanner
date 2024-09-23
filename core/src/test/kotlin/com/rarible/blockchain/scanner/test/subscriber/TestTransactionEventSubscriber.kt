package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.TransactionEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.model.TestTransactionRecord
import java.util.concurrent.ConcurrentHashMap

class TestTransactionEventSubscriber(
    private val eventDataCount: Int = 1,
    private val exceptionProvider: () -> Exception? = { null }
) : TransactionEventSubscriber<TestBlockchainBlock, TestTransactionRecord> {

    private val expectedRecords: MutableMap<TestBlockchainBlock, List<TestTransactionRecord>> =
        ConcurrentHashMap()

    override fun getGroup(): String = "test"

    fun getReturnedRecords(
        testBlockchainBlock: TestBlockchainBlock,
    ): List<TestTransactionRecord> {
        return expectedRecords.getValue(testBlockchainBlock)
    }

    override suspend fun getEventRecords(block: TestBlockchainBlock): List<TestTransactionRecord> {
        val exception = exceptionProvider()
        if (exception != null) {
            throw exception
        }
        expectedRecords[block]?.let { return it }
        val records = (0 until eventDataCount).map { minorIndex ->
            TestTransactionRecord(
                hash = block.hash,
                input = block.testExtra,
            )
        }
        expectedRecords[block] = records
        return records
    }
}
