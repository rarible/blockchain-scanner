package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock
import com.rarible.blockchain.scanner.hedera.utils.consensusTimestampToInstant

fun HederaBlock.toBlockchainBlock(): HederaBlockchainBlock {
    return HederaBlockchainBlock(
        number = number,
        hash = hash,
        parentHash = previousHash,
        timestamp = consensusTimestampToInstant(timestamp.to).epochSecond,
        consensusTimestampFrom = timestamp.from,
        consensusTimestampTo = timestamp.to
    )
}
