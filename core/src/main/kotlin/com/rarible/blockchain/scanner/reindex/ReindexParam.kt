package com.rarible.blockchain.scanner.reindex

interface ReindexParam {

    val range: BlockRange
    val name: String?
    val publishEvents: Boolean
}
