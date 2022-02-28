package com.rarible.blockchain.scanner.ethereum.model

import io.daonomic.rpc.domain.Word
import scalether.domain.Address

data class ReindexParam(
    val range: BlockRange,
    val topics: List<Word>,
    val addresses: List<Address>
)

