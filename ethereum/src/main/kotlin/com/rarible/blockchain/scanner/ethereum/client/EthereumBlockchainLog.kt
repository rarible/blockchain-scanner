package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import scalether.domain.response.Log

class EthereumBlockchainLog(val ethLog: Log) : BlockchainLog {

    override val hash = ethLog.transactionHash().toString()
    override val blockHash = ethLog.blockHash().toString()

}
