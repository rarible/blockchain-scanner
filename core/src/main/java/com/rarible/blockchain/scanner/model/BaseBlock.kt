package com.rarible.blockchain.scanner.model

interface BaseBlock {
    val blockNumber: Long
    val blockHash: String
    val parentBlockHash: String
    val timestamp: Long
}