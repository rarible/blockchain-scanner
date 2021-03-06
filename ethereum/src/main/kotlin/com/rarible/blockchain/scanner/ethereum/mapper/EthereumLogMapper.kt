package com.rarible.blockchain.scanner.ethereum.mapper

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import org.springframework.stereotype.Component

@Component
class EthereumLogMapper : LogMapper<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLog> {

    override fun map(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog,
        index: Int,
        minorIndex: Int,
        descriptor: Descriptor
    ): EthereumLog {
        val ethLog = log.ethLog
        return EthereumLog(
            address = ethLog.address(),
            topic = Word.apply(descriptor.id),
            transactionHash = ethLog.transactionHash().toString(),
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