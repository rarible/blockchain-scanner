package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.configuration.JobProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.configuration.TestBlockchainScannerProperties
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.core.common.nowMillis
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import kotlin.math.abs

fun testDescriptor1(): TestDescriptor {
    return TestDescriptor(
        "test_log_event_1",
        "test_topic_1",
        listOf("contract-one_1", "contract-two_1"),
        TestCustomLogRecord::class.java
    )
}

fun testDescriptor2(): TestDescriptor {
    return TestDescriptor(
        "test_log_event_2",
        "test_topic_2",
        listOf("contract-one_2", "contract-two_2"),
        TestCustomLogRecord::class.java
    )
}

fun defaultTestProperties(): TestBlockchainScannerProperties {
    return TestBlockchainScannerProperties(
        retryPolicy = RetryPolicyProperties(),
        job = JobProperties(),
        monitoring = MonitoringProperties()
    )
}

fun randomNewBlockEvent(number: Long): NewBlockEvent {
    return NewBlockEvent(
        source = Source.BLOCKCHAIN,
        number = number,
        hash = randomBlockHash()
    )
}

fun randomReindexBlockEvent(number: Long): ReindexBlockEvent {
    return ReindexBlockEvent(
        source = Source.BLOCKCHAIN,
        number = number
    )
}

fun randomRevertedBlockEvent(number: Long): RevertedBlockEvent {
    return RevertedBlockEvent(
        source = Source.BLOCKCHAIN,
        number = number,
        hash = randomBlockHash()
    )
}

fun randomBlockchainBlock() = TestBlockchainBlock(randomOriginalBlock())

fun randomOriginalBlock() = randomOriginalBlock(randomPositiveLong())
fun randomOriginalBlock(number: Long) = randomOriginalBlock(randomBlockHash(), number)
fun randomOriginalBlock(hash: String, number: Long) = randomOriginalBlock(hash, number, randomBlockHash())
fun randomOriginalBlock(hash: String, number: Long, parentHash: String?): TestOriginalBlock {
    return TestOriginalBlock(
        number,
        hash,
        parentHash,
        randomPositiveLong(nowMillis().epochSecond),
        randomString(16)
    )
}

fun randomBlockchainLog(block: BlockchainBlock, topic: String) = TestBlockchainLog(randomOriginalLog(block.hash, topic))

fun randomOriginalLog(block: TestOriginalBlock, topic: String) = randomOriginalLog(block.hash, topic)
fun randomOriginalLog(blockHash: String?, topic: String): TestOriginalLog {
    return TestOriginalLog(
        transactionHash = randomLogHash(),
        blockHash = blockHash,
        testExtra = randomString(16),
        logIndex = randomInt(),
        topic = topic
    )
}


fun randomTestLogRecord() = randomTestLogRecord(randomString(), randomString())
fun randomTestLogRecord(
    topic: String,
    blockHash: String,
    status: Log.Status = Log.Status.CONFIRMED
): TestCustomLogRecord {
    val testLog = randomTestLog(topic, blockHash, status)
    return randomTestLogRecord(testLog)
}

fun randomTestLogRecord(log: TestOriginalLog, status: Log.Status): TestCustomLogRecord {
    val testLog = randomTestLog().copy(
        transactionHash = log.transactionHash,
        blockHash = log.blockHash,
        status = status,
        topic = log.topic
    )
    return randomTestLogRecord(testLog)
}

fun randomTestLogRecord(testLog: TestLog): TestCustomLogRecord {
    return TestCustomLogRecord(
        id = randomPositiveLong(),
        version = null,
        logExtra = testLog.extra,
        blockExtra = randomString(16),
        customData = randomString(),
        log = testLog
    )
}

fun randomTestLog() = randomTestLog(randomString(), randomString())
fun randomTestLog(topic: String, blockHash: String, status: Log.Status = Log.Status.CONFIRMED): TestLog {
    return TestLog(
        topic = topic,
        transactionHash = randomString(),
        extra = randomString(16),
        visible = true,
        minorLogIndex = randomPositiveInt(),
        status = status,
        blockHash = blockHash,
        logIndex = randomPositiveInt(),
        index = randomPositiveInt()
    )
}

fun randomBlockchainData(blockCount: Int, logsPerBlock: Int, vararg topics: String): TestBlockchainData {
    val blocks = mutableListOf<TestOriginalBlock>()
    val logs = mutableListOf<TestOriginalLog>()
    var parentHash: String? = null
    for (i in 0L until blockCount) {
        val block = randomOriginalBlock(i).copy(parentHash = parentHash)
        blocks.add(block)
        parentHash = block.hash
        for (j in 0 until logsPerBlock) {
            for (topic in topics) {
                logs.add(randomOriginalLog(block, topic))
            }
        }
    }
    return TestBlockchainData(blocks, logs, blocks)
}

fun randomString() = randomString(8)
fun randomString(length: Int) = RandomStringUtils.randomAlphabetic(length)

fun randomInt() = RandomUtils.nextInt()
fun randomPositiveInt() = abs(randomInt())

fun randomLong() = RandomUtils.nextLong()
fun randomPositiveLong() = abs(randomLong())
fun randomPositiveLong(max: Long) = RandomUtils.nextLong(0, max)

fun randomBlockHash() = "B_" + randomString()
fun randomLogHash() = "L_" + randomString()
