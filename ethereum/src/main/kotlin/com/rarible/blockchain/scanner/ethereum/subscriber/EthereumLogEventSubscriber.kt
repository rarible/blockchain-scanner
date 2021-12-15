package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.core.common.nowMillis

abstract class EthereumLogEventSubscriber :
    LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLog, EthereumLogRecord<*>, EthereumDescriptor> {

    /**
     * Helper override that assigns correct minorLogIndex.
     * You need to override [getEthereumEventRecords] instead.
     */
    final override suspend fun getEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): List<EthereumLogRecord<*>> =
        getEthereumEventRecords(block, log).withIndex().map { (minorLogIndex, record) ->
            record.withLog(record.log.copy(minorLogIndex = minorLogIndex))
        }

    abstract suspend fun getEthereumEventRecords(
        block: EthereumBlockchainBlock,
        log: EthereumBlockchainLog
    ): List<EthereumLogRecord<*>>

    /**
     * Converts Ethereum blockchain log provided by Ethereum client to Ethereum log.
     */
    protected fun mapLog(log: EthereumBlockchainLog): EthereumLog {
        val ethLog = log.ethLog
        val nowInstant = nowMillis()
        return EthereumLog(
            address = ethLog.address(),
            topic = ethLog.topics().head(),
            transactionHash = ethLog.transactionHash().toString(),
            status = Log.Status.CONFIRMED,
            blockHash = ethLog.blockHash(),
            blockNumber = ethLog.blockNumber().toLong(),
            logIndex = ethLog.logIndex().toInt(),
            minorLogIndex = 0, // will be assigned above.
            index = log.index,
            visible = true,
            createdAt = nowInstant,
            updatedAt = nowInstant
        )
    }
}
