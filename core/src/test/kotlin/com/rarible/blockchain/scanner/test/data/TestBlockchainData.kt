package com.rarible.blockchain.scanner.test.data

import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalLog

data class TestBlockchainData(
    val blocks: List<TestOriginalBlock>,
    val logs: List<TestOriginalLog>
)

