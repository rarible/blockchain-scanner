package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.data.randomPositiveLong
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord

class TestLogEventSubscriber(
    private val descriptor: TestDescriptor,
    private val eventDataCount: Int = 1
) : LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {

    val expectedRecords: MutableMap<Pair<TestOriginalBlock, TestOriginalLog>, List<TestLogRecord<*>>> = hashMapOf()

    override fun getDescriptor(): TestDescriptor = descriptor

    override suspend fun getEventRecords(
        block: TestBlockchainBlock,
        log: TestBlockchainLog
    ): List<TestLogRecord<*>> = (0 until eventDataCount).map { minorIndex ->
        TestCustomLogRecord(
            id = randomPositiveLong(),
            version = null,
            blockExtra = block.testOriginalBlock.testExtra,
            logExtra = log.testOriginalLog.testExtra,
            customData = randomString(),
            log = mapLog(log, minorIndex)
        )
    }.also { records ->
        expectedRecords[block.testOriginalBlock to log.testOriginalLog] = records.map { it.copy(version = 0) }
    }


    private fun mapLog(log: TestBlockchainLog, minorLogIndex: Int): TestLog {
        val testLog = log.testOriginalLog
        return TestLog(
            topic = log.testOriginalLog.topic,
            transactionHash = testLog.transactionHash,
            extra = testLog.testExtra,
            visible = true,
            minorLogIndex = minorLogIndex,
            status = Log.Status.CONFIRMED,
            blockHash = testLog.blockHash,
            logIndex = testLog.logIndex,
            index = log.index
        )
    }
}
