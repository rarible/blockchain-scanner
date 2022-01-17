package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator

object EthereumLogRecordComparator : LogRecordComparator<EthereumLogRecord> {
    override fun compare(r1: EthereumLogRecord, r2: EthereumLogRecord): Int {
        return when {
            r1.log.status.isPending() && r2.log.status.isPending() ->
                compareBy<EthereumLogRecord> { it.log.transactionHash }
                    .thenBy { it.log.address.toString() }
                    .thenBy { it.log.topic.toString() }
                    .thenBy { it.log.minorLogIndex }
                    .compare(r1, r2)
            r1.log.status.isPending() -> -1
            r2.log.status.isPending() -> 1
            else -> compareBy<EthereumLogRecord> { it.log.blockNumber!! }
                .thenBy { it.log.logIndex!! }
                .thenBy { it.log.minorLogIndex }
                .compare(r1, r2)
        }
    }

    private fun EthereumLogStatus.isPending(): Boolean {
        return this == EthereumLogStatus.PENDING || this == EthereumLogStatus.DROPPED || this == EthereumLogStatus.INACTIVE
    }
}