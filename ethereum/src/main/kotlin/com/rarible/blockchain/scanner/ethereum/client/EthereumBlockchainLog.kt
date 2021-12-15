package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import scalether.domain.response.Log

data class EthereumBlockchainLog(val ethLog: Log, val index: Int) : BlockchainLog {

    override val hash = ethLog.transactionHash().toString()
    override val blockHash = ethLog.blockHash().toString()

}
