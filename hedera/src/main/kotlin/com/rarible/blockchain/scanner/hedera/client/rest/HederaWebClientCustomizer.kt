package com.rarible.blockchain.scanner.hedera.client.rest

import org.springframework.boot.web.reactive.function.client.WebClientCustomizer
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration

@Component
class HederaWebClientCustomizer : WebClientCustomizer {
    private val timeout: Duration = Duration.ofSeconds(30)

    override fun customize(webClientBuilder: WebClient.Builder) {
        webClientBuilder.codecs { configurer ->
            configurer.defaultCodecs().maxInMemorySize(20 * 1024 * 1024) // 20MB
        }

        val provider = ConnectionProvider.builder("hedera-connection-provider")
            .maxConnections(1024)
            .pendingAcquireMaxCount(-1)
            .maxIdleTime(timeout)
            .maxLifeTime(timeout)
            .lifo()
            .build()

        val client = HttpClient
            .create(provider)
            .responseTimeout(timeout)
            .followRedirect(true)

        val connector = ReactorClientHttpConnector(client)
        webClientBuilder.clientConnector(connector)
    }
}
