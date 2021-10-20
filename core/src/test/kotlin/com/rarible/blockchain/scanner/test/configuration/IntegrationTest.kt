package com.rarible.blockchain.scanner.test.configuration

import com.rarible.core.test.ext.EthereumTest
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import kotlinx.coroutines.FlowPreview
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@FlowPreview
@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [TestScannerConfiguration::class])
@SpringBootTest
@ActiveProfiles("test")
@EthereumTest
annotation class IntegrationTest
