package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomLog
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RComparatorTest {

    @Test
    fun `sort records`() {
        val r1 = record(1, 1, 1)
        val r2 = record(1, 2, 1)
        val r3 = record(1, 2, 2)
        val r4 = record(2, 1, 2)
        val r5 = record(2, 3, 0)

        val list = listOf(r1, r2, r3, r4, r5).shuffled()

        val sorted = list.sortedWith(EthereumLogRecordComparator)

        assertThat(sorted[0]).isEqualTo(r1)
        assertThat(sorted[1]).isEqualTo(r2)
        assertThat(sorted[2]).isEqualTo(r3)
        assertThat(sorted[3]).isEqualTo(r4)
        assertThat(sorted[4]).isEqualTo(r5)
    }

    private fun record(
        blockNumber: Long?,
        logIndex: Int?,
        minorLogIndex: Int,
        status: EthereumBlockStatus = EthereumBlockStatus.CONFIRMED
    ): ReversedEthereumLogRecord {
        return randomLogRecord(
            randomLog(topic = randomWord(), blockHash = randomWord(), status = status)
                .copy(blockNumber = blockNumber, logIndex = logIndex, minorLogIndex = minorLogIndex)
        )
    }
}
