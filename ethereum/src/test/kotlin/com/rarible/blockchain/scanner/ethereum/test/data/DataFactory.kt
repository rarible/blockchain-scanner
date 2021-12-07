package com.rarible.blockchain.scanner.ethereum.test.data

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import scalether.domain.AddressFactory
import java.math.BigInteger
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.abs

fun randomBlock(): EthereumBlock {
    return EthereumBlock(
        id = randomPositiveLong(),
        hash = randomBlockHash().toString(),
        parentHash = randomBlockHash().toString(),
        timestamp = randomPositiveLong()
    )
}

fun randomLogRecord(topic: Word, blockHash: Word, status: Log.Status = Log.Status.CONFIRMED) =
    randomLogRecord(randomLog(topic, blockHash, status))


fun randomLogRecord(log: EthereumLog): TestEthereumLogRecord {
    return TestEthereumLogRecord(
        id = randomString(),
        version = null,
        customData = randomString(),
        log = log,
        from = randomAddress(),
        to = randomAddress(),
        value = BigInteger.valueOf(randomPositiveLong())
    )
}

fun randomLog(topic: Word, blockHash: Word, status: Log.Status = Log.Status.CONFIRMED) =
    randomLog(randomLogHash(), topic, blockHash, status)

fun randomLog(
    transactionHash: String,
    topic: Word,
    blockHash: Word,
    status: Log.Status = Log.Status.CONFIRMED
): EthereumLog {
    return EthereumLog(
        transactionHash = transactionHash,
        blockHash = blockHash,
        status = status,
        address = randomAddress(),
        topic = topic,
        index = randomPositiveInt(),
        logIndex = randomPositiveInt(),
        minorLogIndex = randomPositiveInt()
    )
}

fun randomAddress() = AddressFactory.create()
fun randomWord() = Word.apply(RandomUtils.nextBytes(32))

fun randomString() = randomString(8)
fun randomString(length: Int) = RandomStringUtils.randomAlphabetic(length)

fun randomInt() = RandomUtils.nextInt()
fun randomPositiveInt() = abs(randomInt())

fun randomLong() = RandomUtils.nextLong()
fun randomPositiveLong() = abs(randomLong())

fun randomBigInt() = RandomUtils.nextLong().toBigInteger()
fun randomPositiveBigInt(max: Long) = RandomUtils.nextLong(0, max).toBigInteger()

fun randomBlockHash() = ByteArray(32).let {
    ThreadLocalRandom.current().nextBytes(it)
    Word.apply(it)
}

fun randomLogHash() = Word(RandomUtils.nextBytes(32)).toString()