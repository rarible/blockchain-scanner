package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogEventDescriptor
import com.rarible.blockchain.scanner.test.subscriber.TestEventData
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.bson.types.ObjectId
import kotlin.math.abs

fun testDescriptor1(): TestLogEventDescriptor {
    return TestLogEventDescriptor(
        "test_log_event_1",
        "test_topic_1",
        listOf("contract-one_1", "contract-two_1")
    )
}

fun testDescriptor2(): TestLogEventDescriptor {
    return TestLogEventDescriptor(
        "test_log_event_2",
        "test_topic_2",
        listOf("contract-one_2", "contract-two_2")
    )
}

fun randomTestBlock() = randomTestBlock(randomBlockHash())
fun randomTestBlock(hash: String): TestBlock {
    return TestBlock(
        randomPositiveLong(),
        hash,
        randomPositiveLong(),
        Block.Status.PENDING,
        randomString(16)
    )
}

fun randomBlockchainBlock() = TestBlockchainBlock(randomOriginalBlock())
fun randomBlockchainBlock(hash: String) = TestBlockchainBlock(randomOriginalBlock(hash, randomPositiveLong()))

fun randomOriginalBlock() = randomOriginalBlock(randomPositiveLong())
fun randomOriginalBlock(number: Long) = randomOriginalBlock(randomBlockHash(), number)
fun randomOriginalBlock(hash: String, number: Long): TestOriginalBlock {
    return TestOriginalBlock(
        number,
        hash,
        randomBlockHash(),
        randomPositiveLong(),
        randomString(16)
    )
}

fun randomBlockchainLog(block: BlockchainBlock, topic: String) = TestBlockchainLog(randomOriginalLog(block.hash, topic))
fun randomBlockchainLog(topic: String) = TestBlockchainLog(randomOriginalLog(topic))

fun randomOriginalLog(topic: String) = randomOriginalLog(randomString(), topic)
fun randomOriginalLog(block: TestOriginalBlock, topic: String) = randomOriginalLog(topic, block.hash)
fun randomOriginalLog(blockHash: String, topic: String): TestOriginalLog {
    return TestOriginalLog(
        randomLogHash(),
        blockHash,
        randomString(16),
        randomInt(),
        topic
    )
}

fun randomTestLog(topic: String, blockHash: String): TestLog {
    val data = randomTestData()
    return TestLog(
        id = ObjectId(),
        version = null,
        data = data,
        topic = topic,
        transactionHash = randomString(),
        extra = data.logExtra,
        visible = true,
        minorLogIndex = randomPositiveInt(),
        status = Log.Status.CONFIRMED,
        blockHash = blockHash,
        logIndex = randomPositiveInt(),
        index = randomPositiveInt()
    )
}

fun randomTestData(): TestEventData {
    return TestEventData(
        randomPositiveInt(),
        randomString(16),
        randomString(16)
    )
}

fun randomBlockchainData(blockCount: Int, logsPerBlock: Int, topic: String): TestBlockchainData {
    val blocks = mutableListOf<TestOriginalBlock>()
    val logs = mutableListOf<TestOriginalLog>()
    for (i in 0 until blockCount) {
        val block = randomOriginalBlock(i + 1L)
        blocks.add(block)
        for (j in 0 until logsPerBlock) {
            logs.add(randomOriginalLog(block, topic))
        }
    }
    return TestBlockchainData(blocks, logs)
}

fun randomString() = randomString(8)
fun randomString(length: Int) = RandomStringUtils.randomAlphabetic(length)

fun randomInt() = RandomUtils.nextInt()
fun randomPositiveInt() = abs(randomInt())

fun randomLong() = RandomUtils.nextLong()
fun randomPositiveLong() = abs(randomLong())

fun randomBlockHash() = "B_" + randomString()
fun randomLogHash() = "L_" + randomString()