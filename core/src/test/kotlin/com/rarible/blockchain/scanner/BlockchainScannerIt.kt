package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.configuration.TestScannerConfiguration.Companion.TEST_BLOCK_COUNT
import com.rarible.blockchain.scanner.test.configuration.TestScannerConfiguration.Companion.TEST_LOG_COUNT_PER_BLOCK
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.data.testDescriptor2
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
class BlockchainScannerIt {

    @Autowired
    lateinit var testBlockchainScanner: TestBlockchainScanner

    @Autowired
    lateinit var testBlockRepository: TestBlockRepository

    @Autowired
    lateinit var testLogRepository: TestLogRepository

    @Test
    fun `scan all`() = runBlocking {
        try {
            testBlockchainScanner.scan()
        } catch (e: IllegalStateException) {
            // Do nothing, in prod there will be infinite attempts count
        }

        val expectedLogCount = TEST_BLOCK_COUNT * TEST_LOG_COUNT_PER_BLOCK

        // Since we have 2 subscribers, there should be 2 collections
        val allLogs1 = testLogRepository.findAll(testDescriptor1().collection).awaitFirst().size
        val allLogs2 = testLogRepository.findAll(testDescriptor2().collection).awaitFirst().size
        val allBlocks = testBlockRepository.findAll().awaitFirst().size

        // First subscriber produces 1 event per log
        assertEquals(expectedLogCount, allLogs1)
        // Second subscriber produces 2 event per log
        assertEquals(expectedLogCount * 2, allLogs2)
        // We expect all blocks processed
        assertEquals(TEST_BLOCK_COUNT, allBlocks)
    }

}