package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.LogMeta

interface BlockchainLog {

    val meta: LogMeta

    val hash: String get() = meta.hash
    val blockHash: String get() = meta.blockHash
}