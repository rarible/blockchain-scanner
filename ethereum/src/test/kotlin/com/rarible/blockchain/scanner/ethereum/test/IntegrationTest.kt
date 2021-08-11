package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [TestEthereumScannerConfiguration::class])
@SpringBootTest
@ActiveProfiles("test")
@EnableAutoConfiguration
annotation class IntegrationTest