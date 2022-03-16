package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.repository.SolanaLogRepository
import com.rarible.core.test.data.randomInt
import com.rarible.core.test.data.randomLong
import com.rarible.core.test.data.randomString
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import kotlin.time.ExperimentalTime

@ExperimentalTime
class SolanaLogRepositoryIt : AbstractIntegrationTest() {

    private val collection = "test-records"

    @Autowired
    lateinit var solanaLogRepository: SolanaLogRepository

    @Test
    fun save() = runBlocking<Unit> {
        val solanaLog = createRandomSolanaLog()
        val solanaRecord = TestSolanaLogRecord(solanaLog, randomString())
        solanaLogRepository.save(collection, solanaRecord)
        assertThat(solanaLogRepository.findAll(collection).toList())
            .isEqualTo(listOf(solanaRecord))
    }

    @Test
    fun `save all`() = runBlocking<Unit> {
        val solanaLog = createRandomSolanaLog()
        val solanaRecords = (0 until 10000).map {
            TestSolanaLogRecord(solanaLog, randomString())
        }
        assertThat(solanaLogRepository.saveAll(collection, solanaRecords).toList().sortedBy { it.toString() })
            .isEqualTo(solanaRecords.sortedBy { it.toString() })
        assertThat(solanaLogRepository.findAll(collection).toList().sortedBy { it.toString() })
            .isEqualTo(solanaRecords.sortedBy { it.toString() })
    }

    private fun createRandomSolanaLog() = SolanaLog(
        blockNumber = randomLong(),
        transactionHash = randomString(),
        blockHash = randomString(),
        transactionIndex = randomInt(),
        instructionIndex = randomInt(),
        innerInstructionIndex = randomInt()
    )
}

data class TestSolanaLogRecord(
    override val log: SolanaLog,
    private val recordKey: String
) : SolanaLogRecord() {
    override fun getKey(): String = recordKey
}
