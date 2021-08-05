package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.configuration.TestScannerConfiguration.Companion.TEST_BLOCK_COUNT
import com.rarible.blockchain.scanner.test.configuration.TestScannerConfiguration.Companion.TEST_LOG_COUNT_PER_BLOCK
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.data.testDescriptor2
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
class BlockchainScannerIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var testBlockchainScanner: TestBlockchainScanner

    @Test
    fun `scan all`() = runBlocking {
        scanOnce(testBlockchainScanner)
        val expectedLogCount = TEST_BLOCK_COUNT * TEST_LOG_COUNT_PER_BLOCK

        // Since we have 2 subscribers, there should be 2 collections
        val allLogs1 = findAllLogs(testDescriptor1().collection)
        val allLogs2 = findAllLogs(testDescriptor2().collection)
        val allBlocks = findAllBlocks()

        // First subscriber produces 1 event per log
        assertEquals(expectedLogCount, allLogs1.size)
        // Second subscriber produces 2 event per log
        assertEquals(expectedLogCount * 2, allLogs2.size)
        // We expect all blocks processed
        assertEquals(TEST_BLOCK_COUNT, allBlocks.size)
    }

}