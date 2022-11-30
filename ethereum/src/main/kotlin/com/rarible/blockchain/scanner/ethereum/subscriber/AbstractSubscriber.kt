package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EventData
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.SubscriberGroup
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import scalether.domain.Address
import scalether.domain.response.Log
import scalether.domain.response.Transaction
import java.time.Instant

sealed class AbstractSubscriber<T : EventData>(
    group: SubscriberGroup,
    collection: String,
    topic: Word,
    contracts: List<Address>
) : EthereumLogEventSubscriber() {

    private val descriptor = EthereumDescriptor(
        groupId = group,
        collection = collection,
        ethTopic = topic,
        contracts = contracts,
        entityType = ReversedEthereumLogRecord::class.java
    )

    override fun getDescriptor(): EthereumDescriptor = descriptor

    override suspend fun getEthereumEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): List<EthereumLogRecord> {
        return convert(
            log = log.ethLog,
            transaction = log.ethTransaction,
            timestamp = Instant.ofEpochSecond(block.timestamp),
            index = log.index,
            totalLogs = log.total
        ).map {
                ReversedEthereumLogRecord(
                    id = ObjectId().toHexString(),
                    log = mapLog(block, log),
                    data = it
                )
            }
    }

    protected abstract suspend fun convert(
        log: Log,
        transaction: Transaction,
        timestamp: Instant,
        index: Int,
        totalLogs: Int
    ): List<T>
}