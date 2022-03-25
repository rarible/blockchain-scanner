package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.blockchain.scanner.solana.EnableSolanaScanner
import com.rarible.blockchain.scanner.solana.client.SolanaApi
import com.rarible.blockchain.scanner.solana.client.SolanaHttpRpcApi
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@EnableSolanaScanner
@EnableAutoConfiguration
class TestSolanaScannerConfiguration {
    @Bean
    fun solanaApi(): SolanaApi {
        return SolanaHttpRpcApi(listOf(MAIN_NET_BETA), 3000)
    }

    companion object {
        const val MAIN_NET_BETA = "https://api.mainnet-beta.solana.com"
    }
}
