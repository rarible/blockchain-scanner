package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.data.LogMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainLog

class FlowBlockchainLog(
    blockId: String,
    txId: String
): BlockchainLog {

    override val meta: LogMeta = LogMeta(
        hash = txId,
        blockHash = blockId
    )

}
