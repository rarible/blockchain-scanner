package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.core.common.nowMillis
import com.rarible.core.test.data.randomAddress
import com.rarible.core.test.data.randomInt
import com.rarible.core.test.data.randomLong
import com.rarible.core.test.data.randomString
import com.rarible.core.test.data.randomWord
import io.daonomic.rpc.domain.Word
import scalether.domain.Address
import java.time.Instant

data class ItemEvent(
    override val entityId: String,
    override val log: EthereumLog,
    val supply: Int = 1,
    override val compact: Boolean = false,
) : EthereumEntityEvent<ItemEvent>()

fun createRandomItemEvent(
    transactionSender: Address = randomAddress(),
    supply: Int = randomInt(),
    blockNumber: Long = randomLong(),
    compact: Boolean = false
): ItemEvent {
    return ItemEvent(
        entityId = randomString(),
        log = createRandomEthereumLog(
            transactionSender = transactionSender,
            blockNumber = blockNumber
        ),
        supply = supply,
        compact = compact
    )
}

fun createRandomEthereumLog(
    transactionSender: Address = randomAddress(),
    blockNumber: Long = randomLong(),
    logIndex: Int = randomInt(),
    minorLogIndex: Int = randomInt(),
    status: EthereumBlockStatus = EthereumBlockStatus.values().random(),
): EthereumLog =
    EthereumLog(
        transactionHash = randomWord(),
        status = status,
        address = randomAddress(),
        topic = Word.apply(randomWord()),
        blockHash = Word.apply(randomWord()),
        blockNumber = blockNumber,
        logIndex = logIndex,
        minorLogIndex = minorLogIndex,
        index = randomInt(),
        from = transactionSender,
        blockTimestamp = nowMillis().epochSecond,
        createdAt = nowMillis()
    )

fun ItemEvent.withNewValues(
    status: EthereumBlockStatus? = null,
    createdAt: Instant? = null,
    blockNumber: Long? = null,
    logIndex: Int? = null,
    minorLogIndex: Int? = null,
    address: Address? = null,
    index: Int? = null,
) = copy(log = log.withNewValues(status, createdAt, blockNumber, logIndex, minorLogIndex, index = index))

fun EthereumLog.withNewValues(
    status: EthereumBlockStatus? = null,
    createdAt: Instant? = null,
    blockNumber: Long? = null,
    logIndex: Int? = null,
    minorLogIndex: Int? = null,
    address: Address? = null,
    from: Address? = null,
    index: Int? = null
) = copy(
    status = status ?: this.status,
    createdAt = createdAt ?: this.createdAt,
    blockNumber = blockNumber ?: if (this.blockNumber != null) null else this.blockNumber,
    logIndex = logIndex ?: if (this.logIndex != null) null else this.logIndex,
    index = index ?: this.index,
    minorLogIndex = minorLogIndex ?: this.minorLogIndex,
    address = address ?: this.address,
    from = from ?: this.from
)
