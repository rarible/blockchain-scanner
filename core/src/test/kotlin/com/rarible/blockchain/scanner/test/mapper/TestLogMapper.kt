package com.rarible.blockchain.scanner.test.mapper

import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestLog
import org.bson.types.ObjectId

class TestLogMapper : LogMapper<TestBlockchainBlock, TestBlockchainLog, TestLog> {

    override fun map(
        block: TestBlockchainBlock,
        log: TestBlockchainLog,
        index: Int,
        minorIndex: Int,
        data: EventData,
        descriptor: LogEventDescriptor
    ): TestLog {
        val testLog = log.testOriginalLog
        return TestLog(
            id = ObjectId(),
            version = null,
            data = data,
            topic = descriptor.topic,
            transactionHash = testLog.transactionHash,
            extra = testLog.testExtra,
            visible = true,
            minorLogIndex = minorIndex,
            status = Log.Status.CONFIRMED,
            blockHash = testLog.blockHash,
            logIndex = testLog.logIndex,
            index = index
        )
    }
}