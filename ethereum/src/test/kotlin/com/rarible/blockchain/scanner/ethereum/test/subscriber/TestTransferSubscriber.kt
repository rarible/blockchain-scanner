package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogRecord
import com.rarible.contracts.test.erc20.TransferEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.bson.types.ObjectId

class TestTransferSubscriber : EthereumLogEventSubscriber {

    override fun getDescriptor(): EthereumDescriptor {
        return EthereumDescriptor(
            ethTopic = TransferEvent.id(),
            groupId = "transfer",
            topic = "transfers",
            collection = "transfers",
            contracts = listOf()
        )
    }

    override fun getEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): Flow<EthereumLogRecord<*>> {
        val scalether = TransferEvent.apply(log.ethLog)
        return flowOf(
            TestEthereumLogRecord(
                id = ObjectId(),
                customData = randomString(),
                from = scalether.from(),
                to = scalether.to(),
                value = scalether.value()
            )
        )
    }

}
