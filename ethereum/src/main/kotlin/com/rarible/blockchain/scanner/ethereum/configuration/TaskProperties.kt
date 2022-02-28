package com.rarible.blockchain.scanner.ethereum.configuration

data class TaskProperties(
    val reindex: ReindexTaskProperties = ReindexTaskProperties(),
    val checkBlocks: CheckBlocksTaskProperties = CheckBlocksTaskProperties()
)

