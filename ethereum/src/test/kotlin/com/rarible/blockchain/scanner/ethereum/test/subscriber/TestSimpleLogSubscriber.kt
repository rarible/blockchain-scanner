package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.repository.NoOpEthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import io.daonomic.rpc.domain.Word

object TestSimpleLogSubscriber : EthereumLogEventSubscriber() {
    val topic: Word = randomWord()

    override fun getDescriptor(): EthereumDescriptor {
        return EthereumDescriptor(
            ethTopic = topic,
            groupId = "simple",
            contracts = listOf(randomAddress(), randomAddress()),
            storage = IgnoringEthereumLogRepository,
        )
    }

    override suspend fun getEthereumEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): List<EthereumLogRecord> {
        return listOf(
            randomLogRecord(topic, block.ethBlock.hash(), log.ethTransaction.hash().toString())
        )
    }
}

object IgnoringEthereumLogRepository : EthereumLogRepository by NoOpEthereumLogRepository {
    override suspend fun save(event: EthereumLogRecord): EthereumLogRecord {
        return event
    }

    override suspend fun saveAll(event: Collection<EthereumLogRecord>): List<EthereumLogRecord> {
        return event.toList()
    }
}
