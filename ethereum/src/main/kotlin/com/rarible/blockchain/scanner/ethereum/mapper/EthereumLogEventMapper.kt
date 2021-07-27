package com.rarible.blockchain.scanner.ethereum.mapper

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEvent
import com.rarible.blockchain.scanner.framework.mapper.LogEventMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import org.bson.types.ObjectId
import scalether.domain.response.Block
import scalether.domain.response.Log
import scalether.domain.response.Transaction

class EthereumLogEventMapper : LogEventMapper<Log, Block<Transaction>, EthereumLogEvent> {

    override fun map(
        block: Block<Transaction>,
        log: Log,
        index: Int,
        minorIndex: Int,
        data: EventData,
        descriptor: LogEventDescriptor
    ): EthereumLogEvent {
        return EthereumLogEvent(
            id = ObjectId(),
            version = 0,
            data = data,
            address = log.address().hex(),
            topic = descriptor.topic,
            transactionHash = log.transactionHash().hex(),
            status = LogEvent.Status.CONFIRMED,
            blockHash = log.blockHash(),
            blockNumber = log.blockNumber().toLong(),
            logIndex = log.logIndex().toInt(),
            minorLogIndex = minorIndex,
            index = index,
            visible = true
        )
    }
}