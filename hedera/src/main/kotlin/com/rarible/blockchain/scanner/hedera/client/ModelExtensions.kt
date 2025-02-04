package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock

fun HederaBlock.toBlockchainBlock(): HederaBlockchainBlock {
    return HederaBlockchainBlock(
        number = number,
        hash = hash,
        parentHash = previousHash,
        timestamp = timestamp.from.toLong(),
        consensusTimestampFrom = timestamp.from,
        consensusTimestampTo = timestamp.to
    )
}
