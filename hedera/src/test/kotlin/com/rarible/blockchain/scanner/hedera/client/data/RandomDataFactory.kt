package com.rarible.blockchain.scanner.hedera.client.data

import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionType
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Timestamp
import com.rarible.core.common.nowMillis
import com.rarible.core.test.data.randomInt
import com.rarible.core.test.data.randomLong
import com.rarible.core.test.data.randomString
import java.time.Instant

fun randomConsensusTimestamp(): String {
    val instant = Instant.now()
    val seconds = instant.epochSecond
    val nanos = String.format("%09d", instant.nano)
    return "$seconds.$nanos"
}

fun createRandomHederaTransaction(
    name: String = HederaTransactionType.values().random().value,
) = HederaTransaction(
    bytes = randomString(),
    chargedTxFee = randomLong(),
    consensusTimestamp = randomConsensusTimestamp(),
    entityId = randomString(),
    maxFee = randomLong().toString(),
    memoBase64 = null,
    name = name,
    node = randomString(),
    nonce = randomInt(),
    parentConsensusTimestamp = null,
    result = "success",
    scheduled = false,
    transactionHash = randomString(),
    transactionId = randomString(),
    transfers = emptyList(),
    nftTransfers = emptyList(),
    validDurationSeconds = randomLong().toString(),
    validStartTimestamp = nowMillis().epochSecond.toString()
)

fun createRandomHederaBlock() = HederaBlock(
    count = randomLong(),
    hapiVersion = "1.0",
    hash = randomString(),
    name = randomString(),
    number = randomLong(),
    previousHash = randomString(),
    size = randomLong(),
    timestamp = Timestamp(
        from = randomConsensusTimestamp(),
        to = randomConsensusTimestamp()
    ),
    gasUsed = randomLong(),
    logsBloom = randomString()
)

fun createRandomHederaBlockchainBlock(): HederaBlockchainBlock {
    return HederaBlockchainBlock(
        number = randomLong(),
        hash = randomString(),
        parentHash = randomString(),
        consensusTimestampFrom = randomConsensusTimestamp(),
        consensusTimestampTo = randomConsensusTimestamp(),
        timestamp = nowMillis().epochSecond,
    )
}
