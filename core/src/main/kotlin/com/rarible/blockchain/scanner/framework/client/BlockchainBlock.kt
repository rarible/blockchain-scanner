package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.BlockMeta

interface BlockchainBlock {

    //todo кажется, тоже не особо нужно. просто можно number, hash, parentHash, timestamp реализовать в детях. лишний класс BlockMeta
    val meta: BlockMeta

    //todo это нельзя в type parameter вынести? или не нужно
    val number: Long get() = meta.number
    //todo это тоже и все остальное тоже
    val hash: String get() = meta.hash
    val parentHash: String? get() = meta.parentHash
    val timestamp: Long get() = meta.timestamp

}