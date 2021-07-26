package com.rarible.blockchain.scanner.framework.client

interface BlockchainBlock {

    val number: Long
    val hash: String
    val parentHash: String
    val timestamp: Long


}