package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog

data class TestBlockchainData(
    // All blocks Blockchain has
    val blocks: List<TestOriginalBlock> = listOf(),
    // All logs blockchain has (should belong to blocks above)
    val logs: List<TestOriginalLog> = listOf(),
    // Blocks exist in Blockchain, but not beign "emitted" to listeners yet, should be sublist of first arg
    val newBlocks: List<TestOriginalBlock> = listOf()
)

