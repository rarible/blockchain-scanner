package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.configuration.TestBlockchainScannerProperties
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.core.test.data.randomWord
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
        scan = ScanProperties(),
        monitoring = MonitoringProperties()
    )
}

fun randomBlockchainBlock(
    hash: String = randomBlockHash(),
    number: Long = randomPositiveLong(),
    parentHash: String? = randomBlockHash()
) = TestBlockchainBlock(
    number = number,
    hash = hash,
    parentHash = parentHash,
    timestamp = randomPositiveLong(),
    testExtra = randomString()
)

fun randomBlockchainLog(
    block: BlockchainBlock,
    topic: String,
    index: Int = randomPositiveInt()
) = TestBlockchainLog(randomOriginalLog(block.hash, topic), index = index)

fun randomOriginalLog(block: TestBlockchainBlock, topic: String) = randomOriginalLog(block.hash, topic)
fun randomOriginalLog(blockHash: String?, topic: String): TestOriginalLog {
    return TestOriginalLog(
        transactionHash = randomLogHash(),
        blockHash = blockHash,
        blockNumber = randomPositiveLong(),
        testExtra = randomString(16),
        logIndex = randomInt(),
        topic = topic
    )
}


fun randomTestLogRecord(topic: String, blockHash: String): TestCustomLogRecord {
    val testLog = randomTestLog(topic, blockHash)
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

fun randomTestLog(topic: String, blockHash: String): TestLog {
    return TestLog(
        transactionHash = randomWord(),
        topic = topic,
        extra = randomString(16),
        visible = true,
        minorLogIndex = randomPositiveInt(),
        blockHash = blockHash,
        blockNumber = randomPositiveLong(),
        logIndex = randomPositiveInt(),
        index = randomPositiveInt()
    )
}

fun randomBlockchainData(blockCount: Int, logsPerBlock: Int, vararg topics: String): TestBlockchainData {
    val blocks = mutableListOf<TestBlockchainBlock>()
    val logs = mutableListOf<TestOriginalLog>()
    var parentHash: String? = null
    for (i in 0L until blockCount) {
        val block = randomBlockchainBlock(number = i, parentHash = parentHash)
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
