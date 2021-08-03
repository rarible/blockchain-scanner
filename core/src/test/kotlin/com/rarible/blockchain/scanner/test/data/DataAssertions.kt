package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.model.TestLog
import org.junit.jupiter.api.Assertions.assertEquals

fun assertBlockchainLogAndLogEquals(blockchainLog: TestBlockchainLog, log: TestLog) {
    assertOriginalLogAndLogEquals(blockchainLog.testOriginalLog, log)
}

fun assertBlockchainLogAndLogEquals(
    blockchainBlock: TestBlockchainBlock,
    blockchainLog: TestBlockchainLog,
    log: TestLog
) {
    assertBlockchainLogAndLogEquals(blockchainBlock.testOriginalBlock, blockchainLog.testOriginalLog, log)
}

fun assertOriginalLogAndLogEquals(originalLog: TestOriginalLog, log: TestLog) {
    assertEquals(originalLog.blockHash, log.blockHash)
    assertEquals(originalLog.transactionHash, log.transactionHash)
    assertEquals(originalLog.logIndex, log.logIndex)
    assertEquals(originalLog.testExtra, log.extra)
    //assertEquals(originalLog.testExtra, (log.data as TestEventRecord).logExtra)
}

fun assertBlockchainLogAndLogEquals(originalBlock: TestOriginalBlock, originalLog: TestOriginalLog, log: TestLog) {
    assertOriginalLogAndLogEquals(originalLog, log)
    assertEquals(originalBlock.hash, log.blockHash)
    //assertEquals(originalBlock.testExtra, (log.data as TestEventRecord).blockExtra)
}