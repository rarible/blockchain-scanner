package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EventData
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.SubscriberGroup
import com.rarible.blockchain.scanner.ethereum.model.SubscriberGroupAlias
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import scalether.domain.Address
import scalether.domain.response.Log
import scalether.domain.response.Transaction
import java.time.Instant

@Suppress("MemberVisibilityCanBePrivate")
abstract class AbstractSubscriber<T : EventData>(
    group: SubscriberGroup,
    topic: Word,
    storage: EthereumLogRepository,
    contracts: List<Address>,
    alias: SubscriberGroupAlias? = null
) : EthereumLogEventSubscriber() {

    protected val ethereumDescriptor = EthereumDescriptor(
        groupId = group,
        alias = alias,
        ethTopic = topic,
        contracts = contracts,
        storage = storage,
    )

    override fun getDescriptor(): EthereumDescriptor = ethereumDescriptor

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
