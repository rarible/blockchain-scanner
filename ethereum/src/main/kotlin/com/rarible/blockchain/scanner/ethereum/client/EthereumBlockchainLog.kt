package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.data.LogMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import scalether.domain.response.Log

class EthereumBlockchainLog(val ethLog: Log) : BlockchainLog {

    override val meta: LogMeta = LogMeta(
        hash = ethLog.transactionHash().hex(),
        blockHash = ethLog.blockHash().hex()
    )

}