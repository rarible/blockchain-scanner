package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.configuration.MirrorNodeClientProperties
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration

class HederaWebClientCustomizer(
    private val properties: MirrorNodeClientProperties
) : WebClientCustomizer {
    private val timeout: Duration = properties.timeout

    override fun customize(webClientBuilder: WebClient.Builder) {
        webClientBuilder.codecs { configurer ->
            configurer.defaultCodecs().maxInMemorySize(properties.maxBodySize.toBytes().toInt())
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
