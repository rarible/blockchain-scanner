package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.LogMeta

interface BlockchainLog {

    //todo вот такая штука, кажется, не очень нужна. можно же просто оставить hash и blockHash и они будут реализованы детьми
    val meta: LogMeta

    val hash: String get() = meta.hash
    val blockHash: String get() = meta.blockHash
}