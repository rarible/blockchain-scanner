package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord

class TestBidSubscriber(
    private val bidRepository: EthereumLogRepository,
) : EthereumLogEventSubscriber() {

    override fun getDescriptor(): EthereumDescriptor {
        return EthereumDescriptor(
            ethTopic = randomWord(),
            groupId = "bids",
            collection = "bids",
            contracts = listOf(randomAddress(), randomAddress()),
            entityType = ReversedEthereumLogRecord::class.java,
            storage = bidRepository,
        )
    }

    override suspend fun getEthereumEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): List<EthereumLogRecord> = emptyList()
}
