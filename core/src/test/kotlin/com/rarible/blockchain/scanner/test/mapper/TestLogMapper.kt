package com.rarible.blockchain.scanner.test.mapper

import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestLog

class TestLogMapper : LogMapper<TestBlockchainBlock, TestBlockchainLog, TestLog> {

    override fun map(
        block: TestBlockchainBlock,
        log: TestBlockchainLog,
        minorIndex: Int,
        descriptor: Descriptor
    ): TestLog {
        val testLog = log.testOriginalLog
        return TestLog(
            topic = descriptor.id,
            transactionHash = testLog.transactionHash,
            extra = testLog.testExtra,
            visible = true,
            minorLogIndex = minorIndex,
            status = Log.Status.CONFIRMED,
            blockHash = testLog.blockHash,
            logIndex = testLog.logIndex,
            index = log.index
        )
    }
}
