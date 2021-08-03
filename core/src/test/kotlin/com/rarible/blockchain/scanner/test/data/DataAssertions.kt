package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import org.junit.jupiter.api.Assertions.assertEquals

fun assertRecordAndLogEquals(testLogRecord: TestLogRecord<*>, blockchainLog: TestOriginalLog) {
    assertOriginalLogAndLogEquals(blockchainLog, testLogRecord.log!!)
    assertEquals(blockchainLog.testExtra, testLogRecord.logExtra)
}

fun assertRecordAndLogEquals(
    testLogRecord: TestLogRecord<*>,
    blockchainLog: TestOriginalLog,
    blockchainBlock: TestOriginalBlock
) {
    assertRecordAndLogEquals(testLogRecord, blockchainLog)
    assertEquals(testLogRecord.blockExtra, blockchainBlock.testExtra)
}

fun assertBlockchainLogAndLogEquals(
    blockchainLog: TestBlockchainLog,
    log: TestLog,
    blockchainBlock: TestBlockchainBlock
) {
    assertOriginalLogAndLogEquals(blockchainLog.testOriginalLog, log, blockchainBlock.testOriginalBlock)
}

fun assertBlockchainLogAndLogEquals(blockchainLog: TestBlockchainLog, log: TestLog) {
    assertOriginalLogAndLogEquals(blockchainLog.testOriginalLog, log)
}

fun assertOriginalLogAndLogEquals(originalLog: TestOriginalLog, log: TestLog, originalBlock: TestOriginalBlock) {
    assertOriginalLogAndLogEquals(originalLog, log)
    assertEquals(originalBlock.hash, log.blockHash)
}

fun assertOriginalLogAndLogEquals(originalLog: TestOriginalLog, log: TestLog) {
    assertEquals(originalLog.blockHash, log.blockHash)
    assertEquals(originalLog.transactionHash, log.transactionHash)
    assertEquals(originalLog.logIndex, log.logIndex)
    assertEquals(originalLog.testExtra, log.extra)
    assertEquals(originalLog.topic, log.topic)
}

