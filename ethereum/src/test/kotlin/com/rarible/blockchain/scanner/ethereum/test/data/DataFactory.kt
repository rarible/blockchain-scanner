package com.rarible.blockchain.scanner.ethereum.test.data

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.core.common.nowMillis
import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import scala.jdk.javaapi.CollectionConverters
import scalether.domain.Address
import scalether.domain.AddressFactory
import scalether.domain.response.Block
import scalether.domain.response.Transaction
import java.math.BigInteger
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.abs

fun randomBlock(): com.rarible.blockchain.scanner.block.Block {
    return com.rarible.blockchain.scanner.block.Block(
        id = randomPositiveLong(),
        hash = randomBlockHash().toString(),
        parentHash = randomBlockHash().toString(),
        timestamp = randomPositiveLong(),
        status = BlockStatus.PENDING
    )
}

fun randomLogRecord(
    topic: Word,
    blockHash: Word?,
    transactionHash: String = randomLogHash(),
    status: EthereumLogStatus = EthereumLogStatus.CONFIRMED
) = randomLogRecord(randomLog(transactionHash, topic, blockHash, status = status))

fun randomLogRecord(log: EthereumLog): ReversedEthereumLogRecord {
    return ReversedEthereumLogRecord(
        id = randomString(),
        version = null,
        log = log,
        data = TestEthereumLogData(
            customData = randomString(),
            to = randomAddress(),
            from = randomAddress(),
            value = BigInteger.valueOf(randomPositiveLong()),
            transactionInput = randomString()
        )
    )
}


fun randomLog(
    topic: Word,
    blockHash: Word,
    address: Address = randomAddress(),
    status: EthereumLogStatus = EthereumLogStatus.CONFIRMED
) = randomLog(randomLogHash(), topic, blockHash, address, status)

fun randomLog(
    transactionHash: String,
    topic: Word,
    blockHash: Word?,
    address: Address = randomAddress(),
    status: EthereumLogStatus = EthereumLogStatus.CONFIRMED
): EthereumLog {
    return EthereumLog(
        transactionHash = transactionHash,
        blockHash = blockHash,
        status = status,
        address = address,
        topic = topic,
        index = randomPositiveInt(),
        logIndex = randomPositiveInt(),
        minorLogIndex = randomPositiveInt(),
        blockTimestamp = randomPositiveLong(),
        from = randomAddress(),
        createdAt = nowMillis()
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

fun ethLog(
    transactionHash: Word,
    topic: Word,
    address: Address,
    logIndex: Int,
    blockHash: Word
) = scalether.domain.response.Log(
    logIndex.toBigInteger(),
    randomPositiveBigInt(100),
    transactionHash,
    blockHash,
    randomPositiveBigInt(100),
    address,
    Binary.apply("0x0"),
    false,
    CollectionConverters.asScala(listOf(topic)).toList(),
    randomString()
)

fun ethTransaction(
    transactionHash: Word = randomWord(),
    blockHash: Word = randomWord(),
    blockNumber: BigInteger = randomPositiveBigInt(100)
) = Transaction(
    transactionHash,
    BigInteger.ZERO,
    blockHash,
    blockNumber,
    Address.ZERO(),
    BigInteger.ZERO,
    randomAddress(),
    randomAddress(),
    BigInteger.ZERO,
    BigInteger.ZERO,
    BigInteger.ZERO,
    Binary.empty()
)

fun ethBlock(number: Int, hash: Word, logs: List<EthereumBlockchainLog> = emptyList()): Block<Transaction> = Block(
    number.toBigInteger(),
    hash,
    randomWord(),
    randomString(),
    randomString(),
    randomString(),
    randomString(),
    randomString(),
    com.rarible.core.test.data.randomAddress(),
    randomPositiveBigInt(1),
    randomPositiveBigInt(1),
    Binary.empty(),
    BigInteger.ZERO,
    BigInteger.ZERO,
    BigInteger.ZERO,
    CollectionConverters.asScala(
        logs.filter {
            it.ethTransaction.blockNumber() == number.toBigInteger()
                && it.ethTransaction.blockHash() == hash
        }.map { it.ethTransaction }
    ).toList(),
    BigInteger.ZERO,
)
