package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.data.LogMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import org.bouncycastle.util.encoders.Hex
import com.nftco.flow.sdk.FlowEvent

class FlowBlockchainLog(
    override val hash: String,
    override val blockHash: String?,
    val event: FlowEvent?,
    val errorMessage: String?
): BlockchainLog {

    override val meta: LogMeta = LogMeta(
        hash = hash,
        blockHash = blockHash
    )
}
