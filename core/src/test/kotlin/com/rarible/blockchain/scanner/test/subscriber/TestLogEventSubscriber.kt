package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
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

    override fun getDescriptor(): TestDescriptor {
        return descriptor
    }

    override suspend fun getEventRecords(
        block: TestBlockchainBlock,
        log: TestBlockchainLog,
        logMapper: LogMapper<TestBlockchainBlock, TestBlockchainLog, TestLog>,
        index: Int
    ): List<TestLogRecord<*>> {
        val eventDataList = ArrayList<TestLogRecord<*>>(eventDataCount)
        for (i in 0 until eventDataCount) {
            val record = TestCustomLogRecord(
                id = randomPositiveLong(),
                version = null,
                blockExtra = block.testOriginalBlock.testExtra,
                logExtra = log.testOriginalLog.testExtra,
                customData = randomString(),
                log = logMapper.map(block, log, index, i, descriptor)
            )
            eventDataList.add(record)
        }
        return eventDataList
    }
}
