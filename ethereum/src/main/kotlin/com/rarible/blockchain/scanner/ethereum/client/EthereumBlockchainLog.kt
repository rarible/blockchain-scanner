package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.LogMeta
import scalether.domain.response.Log

class EthereumBlockchainLog(val ethLog: Log) : BlockchainLog {

    override val meta: LogMeta = LogMeta(
        hash = ethLog.transactionHash().toString(),
        blockHash = ethLog.blockHash().toString()
    )

}
