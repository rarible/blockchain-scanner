package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator

object EthereumLogEventComparator : LogEventComparator<EthereumLog, EthereumLogRecord<*>> {
    override fun compare(r1: EthereumLogRecord<*>, r2: EthereumLogRecord<*>): Int {
        return when {
            r1.log.status == EthereumLogStatus.PENDING && r2.log.status == EthereumLogStatus.PENDING ->
                compareBy<EthereumLogRecord<*>> { it.log.transactionHash }
                    .thenBy { it.log.address.toString() }
                    .thenBy { it.log.topic.toString() }
                    .thenBy { it.log.minorLogIndex }
                    .compare(r1, r2)
            r1.log.status == EthereumLogStatus.PENDING -> -1
            r2.log.status == EthereumLogStatus.PENDING -> 1
            else -> compareBy<EthereumLogRecord<*>> { it.log.blockNumber!! }
                .thenBy { it.log.logIndex!! }
                .thenBy { it.log.minorLogIndex }
                .compare(r1, r2)
        }
    }
}
