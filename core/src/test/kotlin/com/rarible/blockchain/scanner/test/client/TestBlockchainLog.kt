package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog

data class TestBlockchainLog(val testOriginalLog: TestOriginalLog) : BlockchainLog {

    override val hash = testOriginalLog.transactionHash
    override val blockHash = testOriginalLog.blockHash
    override val index = 0

}
