package com.rarible.blockchain.scanner.flow

import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.junit.jupiter.Testcontainers


@SpringBootTest(properties = [
    "rarible.task.initialDelay=0",
    "blockchain.scanner.flow.chainId=EMULATOR",
    "blockchain.scanner.flow.poller.delay=200",
/*    "spring.data.mongodb.database=test",
    "logging.level.com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller=DEBUG"*/])
@ContextConfiguration(classes = [TestConfig::class])
@MongoCleanup
@MongoTest
@Testcontainers
class TestnetCommonNFTReadTest {
}
