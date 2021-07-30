package com.rarible.blockchain.scanner.ethereum.mapper

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import org.bson.types.ObjectId
import org.springframework.stereotype.Component

@Component
class EthereumLogMapper : LogMapper<EthereumBlockchainLog, EthereumBlockchainBlock, EthereumLog> {

    override fun map(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog,
        index: Int,
        minorIndex: Int,
        data: EventData,
        descriptor: LogEventDescriptor
    ): EthereumLog {
        val ethLog = log.ethLog
        return EthereumLog(
            id = ObjectId(),
            version = 0,
            data = data,
            address = ethLog.address().hex(),
            topic = descriptor.topic,
            transactionHash = ethLog.transactionHash().hex(),
            status = Log.Status.CONFIRMED,
            blockHash = ethLog.blockHash(),
            blockNumber = ethLog.blockNumber().toLong(),
            logIndex = ethLog.logIndex().toInt(),
            minorLogIndex = minorIndex,
            index = index,
            visible = true
        )
    }
}