package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow

class TestBidSubscriber : EthereumLogEventSubscriber {

    override fun getDescriptor(): EthereumDescriptor {
        return EthereumDescriptor(
            ethTopic = randomWord(),
            groupId = "bids",
            collection = "bids",
            contracts = listOf(randomAddress(), randomAddress()),
            entityType = TestEthereumLogRecord::class.java
        )
    }

    override fun getEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): Flow<EthereumLogRecord<*>> {
        return emptyFlow()
    }

}
