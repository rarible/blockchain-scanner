package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog

class TestBlockchainLog(val testOriginalLog: TestOriginalLog) : BlockchainLog {

    override val hash = testOriginalLog.transactionHash
    override val blockHash = testOriginalLog.blockHash

}
