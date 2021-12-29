package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class TestBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    val testExtra: String
) : BlockchainBlock
