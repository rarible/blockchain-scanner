package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.solana.client.test.TestSolanaLogRecord
import com.rarible.blockchain.scanner.solana.client.test.testRecordsCollection
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.repository.SolanaLogRepository
import com.rarible.core.test.data.randomInt
import com.rarible.core.test.data.randomLong
import com.rarible.core.test.data.randomString
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import kotlin.time.ExperimentalTime

@ExperimentalTime
class SolanaLogRepositoryIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var solanaLogRepository: SolanaLogRepository

    @Test
    fun `mongo indexes`() = runBlocking<Unit> {
        val indexInfos = mongo.indexOps(testRecordsCollection).indexInfo.asFlow().toSet()
        assertThat(indexInfos.map { it.name }.toSet())
            .isEqualTo(
                setOf(
                    "_id_"
                )
            )
    }

    @Test
    fun save() = runBlocking<Unit> {
        val solanaLog = createRandomSolanaLog()
        val solanaRecord = TestSolanaLogRecord(solanaLog, randomString())
        solanaLogRepository.save(testRecordsCollection, solanaRecord)
        assertThat(solanaLogRepository.findAll(testRecordsCollection).toList())
            .isEqualTo(listOf(solanaRecord))
    }

    @Test
    fun `save all`() = runBlocking<Unit> {
        val solanaRecords = (0 until 10000).map {
            TestSolanaLogRecord(createRandomSolanaLog(), randomString())
        }
        assertThat(
            solanaLogRepository.saveAll(testRecordsCollection, solanaRecords).sortedBy { it.toString() })
            .isEqualTo(solanaRecords.sortedBy { it.toString() })
        assertThat(solanaLogRepository.findAll(testRecordsCollection).toList().sortedBy { it.toString() })
            .isEqualTo(solanaRecords.sortedBy { it.toString() })
    }

    @Test
    fun `save all with override of an existing record`() = runBlocking<Unit> {
        val oldRecord = TestSolanaLogRecord(createRandomSolanaLog(), randomString())
        solanaLogRepository.save(testRecordsCollection, oldRecord)
        val newRecords = (0 until 10).map {
            TestSolanaLogRecord(createRandomSolanaLog(), randomString())
        } + listOf(oldRecord)
        solanaLogRepository.saveAll(testRecordsCollection, newRecords)
        assertThat(solanaLogRepository.findAll(testRecordsCollection).toList().sortedBy { it.log })
            .isEqualTo(newRecords.sortedBy { it.log })
    }

    @Test
    fun remove() = runBlocking<Unit> {
        val savedRecord = TestSolanaLogRecord(createRandomSolanaLog(), randomString())
        solanaLogRepository.save(testRecordsCollection, savedRecord)
        val record = solanaLogRepository.findAll(testRecordsCollection).single()
        assertThat(record).isEqualTo(savedRecord)
        solanaLogRepository.delete(testRecordsCollection, record)
        assertThat(solanaLogRepository.findAll(testRecordsCollection).toList()).isEmpty()
    }

    private fun createRandomSolanaLog() = SolanaLog(
        blockNumber = randomLong(1_000_000),
        transactionHash = randomString(44),
        blockHash = randomString(44),
        transactionIndex = randomInt(100),
        instructionIndex = randomInt(100),
        innerInstructionIndex = randomInt(100)
    )
}
