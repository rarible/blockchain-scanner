package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumLogMapper
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.contracts.test.erc20.TransferEvent

class TestTransferSubscriber : EthereumLogEventSubscriber {

    override fun getDescriptor(): EthereumDescriptor {
        return EthereumDescriptor(
            ethTopic = TransferEvent.id(),
            groupId = "transfers",
            collection = "transfers",
            contracts = listOf(),
            entityType = ReversedEthereumLogRecord::class.java
        )
    }

    override suspend fun getEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog,
        logMapper: LogMapper<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLog>,
        index: Int
    ): List<EthereumLogRecord<*>> {
        val scalether = TransferEvent.apply(log.ethLog)
        return listOf(
            ReversedEthereumLogRecord(
                id = randomString(),
                log = EthereumLogMapper().map(block, log, index, 0, getDescriptor()),
                data = TestEthereumLogData(
                    customData = randomString(),
                    to = scalether.to(),
                    from = scalether.from(),
                    value = scalether.value()
                )
            )
        )
    }

}
