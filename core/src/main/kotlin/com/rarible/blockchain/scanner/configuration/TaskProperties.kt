package com.rarible.blockchain.scanner.configuration

data class TaskProperties(
    val reindex: ReindexTaskProperties = ReindexTaskProperties(),
    val checkBlocks: CheckBlocksTaskProperties = CheckBlocksTaskProperties()
)
