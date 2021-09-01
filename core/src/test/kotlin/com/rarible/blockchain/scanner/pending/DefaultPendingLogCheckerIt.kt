package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.*
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventListener
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class DefaultPendingLogCheckerIt : AbstractIntegrationTest() {

    @Autowired
    @Qualifier("testSubscriber1")
    lateinit var subscriber1: TestLogEventSubscriber

    @Autowired
    @Qualifier("testSubscriber2")
    lateinit var subscriber2: TestLogEventSubscriber

    private var blockListener: BlockListener = mockk()
    private var listener: TestLogEventListener = spyk()

    private var topic1 = ""
    private var collection1 = ""
    private var topic2 = ""
    private var collection2 = ""

    @BeforeEach
    fun beforeEach() {
        clearMocks(blockListener)
        clearMocks(listener)
        coEvery { blockListener.onBlockEvent(any()) } returns Unit
        coEvery { listener.onPendingLogsDropped(any()) } returns Unit

        topic1 = subscriber1.getDescriptor().topic
        collection1 = subscriber1.getDescriptor().collection
        topic2 = subscriber2.getDescriptor().topic
        collection2 = subscriber2.getDescriptor().collection
    }

    @Test
    fun test() = runBlocking {

        val block1 = saveBlock(randomOriginalBlock())
        val block2 = saveBlock(randomOriginalBlock())


        val log1 = randomOriginalLog(block1.hash, topic1)
        val log2 = randomOriginalLog(block2.hash, topic1)
        val log3 = randomOriginalLog(block2.hash, topic2)
        val log4 = randomOriginalLog(null, topic2)

        val logRecord1 = saveLog(collection1, randomTestLogRecord(log1, Log.Status.PENDING))
        val logRecord2 = saveLog(collection2, randomTestLogRecord(log2, Log.Status.PENDING))
        val logRecord3 = saveLog(collection2, randomTestLogRecord(log3, Log.Status.INACTIVE))
        val logRecord4 = saveLog(collection2, randomTestLogRecord(log4, Log.Status.PENDING))

        val testBlockchainData = TestBlockchainData(
            listOf(block1, block2),
            listOf(log1, log3, log4)
        )

        val checker = createPendingLogChecker(TestBlockchainClient(testBlockchainData))
        checker.checkPendingLogs()

        // Pending log required reindex should be in the same state since entire block will be reindexed
        assertEquals(findLog(collection1, logRecord1.id)!!.log!!.status, Log.Status.PENDING)
        // For this log there is no data in Blockchain, should be DROPPED
        assertEquals(findLog(collection2, logRecord2.id)!!.log!!.status, Log.Status.DROPPED)
        // Not a pending log, should be the same
        assertEquals(findLog(collection2, logRecord3.id)!!.log!!.status, Log.Status.INACTIVE)
        // LogEvent without block - still waiting to be attached to the block, ignored
        assertEquals(findLog(collection2, logRecord4.id)!!.log!!.status, Log.Status.PENDING)

        // Only block contains pending events must be reindexed
        coVerify(exactly = 1) { blockListener.onBlockEvent(any()) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(block1, null, Source.PENDING)) }

        // Listener notified about dropped logs
        coVerify(exactly = 1) {
            listener.onPendingLogsDropped(match {
                assertEquals(1, it.size)
                assertEquals(logRecord2.log!!.transactionHash, it[0].log!!.transactionHash)
                true
            })
        }
    }

    private fun createPendingLogChecker(
        testBlockchainClient: TestBlockchainClient
    ): DefaultPendingLogChecker<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {

        return DefaultPendingLogChecker(
            testBlockchainClient,
            testLogService,
            listOf(testDescriptor1(), testDescriptor2()),
            blockListener,
            listOf(listener)
        )
    }

}
