package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator

interface EthereumLogEventComparator : LogEventComparator<EthereumLog, EthereumLogRecord<*>> {

    fun compareMinorLogIndex(r1: EthereumLogRecord<*>, r2: EthereumLogRecord<*>): Int {
        val logIndex1 = r1.log.minorLogIndex
        val logIndex2 = r2.log.minorLogIndex
        return logIndex1.compareTo(logIndex2)
    }

    fun compareLogIndex(r1: EthereumLogRecord<*>, r2: EthereumLogRecord<*>): Int {
        val logIndex1 = r1.log.logIndex ?: -1
        val logIndex2 = r2.log.logIndex ?: -1
        return logIndex1.compareTo(logIndex2)
    }

    fun compareBlockNumber(r1: EthereumLogRecord<*>, r2: EthereumLogRecord<*>): Int {
        val blockNumber1 = r1.log.blockNumber ?: -1 // Pending logs should be first in result list
        val blockNumber2 = r2.log.blockNumber ?: -1
        return blockNumber1.compareTo(blockNumber2)
    }

}