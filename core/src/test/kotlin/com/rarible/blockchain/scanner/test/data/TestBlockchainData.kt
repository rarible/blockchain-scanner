package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog

data class TestBlockchainData(
    val blocks: List<TestBlockchainBlock> = listOf(),
    val logs: List<TestOriginalLog> = listOf(),
    val newBlocks: List<TestBlockchainBlock> = listOf()
)
