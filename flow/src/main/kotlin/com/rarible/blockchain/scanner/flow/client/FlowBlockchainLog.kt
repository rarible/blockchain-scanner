package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.data.LogMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import org.bouncycastle.util.encoders.Hex
import org.onflow.sdk.FlowEvent

class FlowBlockchainLog(
    val event: FlowEvent
): BlockchainLog {

    override val meta: LogMeta = LogMeta(
        hash = Hex.toHexString(event.transactionId.bytes),
        blockHash = null
    )

}
