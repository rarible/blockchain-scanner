package com.rarible.blockchain.scanner.ethereum.configuration

data class CheckBlocksTaskProperties(
    val enabled: Boolean = false,
    val reindexBlocks: Boolean = false
)