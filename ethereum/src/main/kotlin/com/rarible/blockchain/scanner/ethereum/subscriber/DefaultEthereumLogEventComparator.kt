package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.stereotype.Component

@Component
@ConditionalOnMissingBean(EthereumLogEventComparator::class)
class DefaultEthereumLogEventComparator : EthereumLogEventComparator {

    override fun compare(r1: EthereumLogRecord<*>, r2: EthereumLogRecord<*>): Int {
        var result = compareBlockNumber(r1, r2)
        if (result == 0) result = compareLogIndex(r1, r2)
        if (result == 0) result = compareMinorLogIndex(r1, r2)
        return result
    }

}