package com.rarible.blockchain.scanner.model

data class BlockEvent<T : BaseBlock>(
    val block: T,
    val reverted: BlockInfo? = null
)