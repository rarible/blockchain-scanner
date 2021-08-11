package com.rarible.blockchain.scanner.test.configuration

import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [TestScannerConfiguration::class])
@SpringBootTest
@ActiveProfiles("test")
annotation class IntegrationTest