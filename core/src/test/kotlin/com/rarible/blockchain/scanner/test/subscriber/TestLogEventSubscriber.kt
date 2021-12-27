package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.data.randomLogHash
import com.rarible.blockchain.scanner.test.data.randomPositiveLong
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord

class TestLogEventSubscriber(
    private val descriptor: TestDescriptor,
    private val eventDataCount: Int = 1
) : LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor> {

    private val expectedRecords: MutableMap<Pair<TestBlockchainBlock, TestOriginalLog>, List<TestLogRecord>> =
        hashMapOf()

    fun getReturnedRecords(
        testBlockchainBlock: TestBlockchainBlock,
        testOriginalLog: TestOriginalLog
    ): List<TestLogRecord> {
        return expectedRecords.getValue(testBlockchainBlock to testOriginalLog)
    }

    override fun getDescriptor(): TestDescriptor = descriptor

    override suspend fun getEventRecords(
        block: TestBlockchainBlock,
        log: TestBlockchainLog
    ): List<TestLogRecord> = (0 until eventDataCount).map { minorIndex ->
        TestCustomLogRecord(
            id = randomPositiveLong(),
            version = null,
            blockExtra = block.testExtra,
            logExtra = log.testOriginalLog.testExtra,
            customData = randomString(),
            log = mapLog(log, minorIndex)
        )
    }.also { records ->
        expectedRecords[block to log.testOriginalLog] = records
    }


    private fun mapLog(log: TestBlockchainLog, minorLogIndex: Int): TestLog {
        val testLog = log.testOriginalLog
        return TestLog(
            transactionHash = randomLogHash(),
            topic = log.testOriginalLog.topic,
            extra = testLog.testExtra,
            visible = true,
            minorLogIndex = minorLogIndex,
            blockHash = testLog.blockHash,
            blockNumber = testLog.blockNumber,
            logIndex = testLog.logIndex,
            index = log.index
        )
    }
}
