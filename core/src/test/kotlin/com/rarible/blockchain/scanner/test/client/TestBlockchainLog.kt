package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.LogMeta

class TestBlockchainLog(val testOriginalLog: TestOriginalLog) : BlockchainLog {

    override val meta: LogMeta = LogMeta(
        hash = testOriginalLog.transactionHash,
        blockHash = testOriginalLog.blockHash
    )

}
