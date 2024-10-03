package com.rarible.blockchain.scanner.test

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.model.TestTransactionRecord
import com.rarible.blockchain.scanner.test.repository.TestLogStorage

class TestBlockchainScanner(
    manager: TestBlockchainScannerManager
) : BlockchainScanner<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestTransactionRecord, TestDescriptor, TestLogStorage>(
    manager
)
