package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.blockchain.scanner.solana.EnableSolanaScanner
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Configuration

@Configuration
@EnableSolanaScanner
@EnableAutoConfiguration
class TestSolanaScannerConfiguration
