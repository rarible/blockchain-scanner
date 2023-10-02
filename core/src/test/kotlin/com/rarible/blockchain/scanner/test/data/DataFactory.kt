package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.handler.BlockEventResult
import com.rarible.blockchain.scanner.handler.BlockListenerResult
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog
import com.rarible.core.common.nowMillis
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.assertj.core.api.Assertions.assertThat
import kotlin.math.abs

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

fun randomOriginalLog(
    block: BlockchainBlock,
    topic: String,
    logIndex: Int
): TestOriginalLog {
    return TestOriginalLog(
        transactionHash = randomLogHash(),
        blockHash = block.hash,
        blockNumber = block.number,
        testExtra = randomString(16),
        logIndex = logIndex,
        topic = topic
    )
}

fun buildBlockchain(blocks: List<TestBlockchainBlock>): List<TestBlockchainBlock> {
    val first = blocks.first()
    assertThat(first.number).isEqualTo(0)
    assertThat(first.parentHash).isNull()

    val blockMap = blocks.associateBy { it.number }

    return blocks.map { block ->
        if (first == block) {
            first
        } else {
            val parentHash = requireNotNull(blockMap[block.number - 1]).hash
            block.copy(parentHash = parentHash)
        }
    }
}

fun randomBlockchain(blockCount: Int): List<TestBlockchainBlock> {
    val blocks = (1L..blockCount).map {
        randomBlockchainBlock(number = it)
    }
    return buildBlockchain(listOf(randomBlockchainBlock(number = 0, parentHash = null)) + blocks)
}

fun randomBlock(
    id: Long = randomPositiveLong(),
    hash: String = randomBlockHash(),
    parentHash: String = randomBlockHash(),
    timestamp: Long = nowMillis().toEpochMilli() - randomPositiveLong(10000000L),
    status: BlockStatus = BlockStatus.SUCCESS
): Block {
    return Block(
        id = id,
        hash = hash,
        parentHash = parentHash,
        timestamp = timestamp,
        status = status,
        errors = emptyList()
    )
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

fun stubListenerResult(blockNumbers: Collection<Long>, stats: BlockStats = BlockStats.empty()) = BlockListenerResult(
    blockNumbers.map { BlockEventResult(it, emptyList(), stats) }
) {}
