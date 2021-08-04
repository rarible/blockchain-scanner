package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog

data class TestBlockchainData(
    val blocks: List<TestOriginalBlock> = listOf(),
    val logs: List<TestOriginalLog> = listOf(),
    val newBlocks: List<TestOriginalBlock> = listOf()
)

