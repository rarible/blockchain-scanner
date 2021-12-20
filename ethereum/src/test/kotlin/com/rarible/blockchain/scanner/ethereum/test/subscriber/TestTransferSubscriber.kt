package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import com.rarible.contracts.test.erc20.TransferEvent

class TestTransferSubscriber : EthereumLogEventSubscriber() {

    override fun getDescriptor(): EthereumDescriptor {
        return EthereumDescriptor(
            ethTopic = TransferEvent.id(),
            groupId = "transfers",
            collection = "transfers",
            contracts = listOf(),
            entityType = ReversedEthereumLogRecord::class.java
        )
    }

    override suspend fun getEthereumEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): List<EthereumLogRecord<*>> {
        val rawEvent = TransferEvent.apply(log.ethLog)
        return listOf(
            ReversedEthereumLogRecord(
                id = randomString(),
                log = mapLog(block, log),
                data = TestEthereumLogData(
                    customData = randomString(),
                    to = rawEvent.to(),
                    from = rawEvent.from(),
                    value = rawEvent.value(),
                    transactionInput = log.ethTransaction.input().toString()
                )
            )
        )
    }

}
