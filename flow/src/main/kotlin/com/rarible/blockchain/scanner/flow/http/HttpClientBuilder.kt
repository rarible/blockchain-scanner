package com.rarible.blockchain.scanner.flow.http

import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.util.unit.DataSize
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import reactor.netty.transport.ProxyProvider
import java.net.URI
import java.time.Duration

object HttpClientBuilder {

    fun build(proxy: URI?): WebClient {
        val webClientBuilder = WebClient.builder()

        val provider = ConnectionProvider.builder("default-connection-provider")
            .maxConnections(300)
            .pendingAcquireMaxCount(-1)
            .maxIdleTime(DEFAULT_TIMEOUT)
            .maxLifeTime(DEFAULT_TIMEOUT)
            .lifo()
            .build()

        val client = HttpClient
            .create(provider)
            .responseTimeout(DEFAULT_TIMEOUT)
            .followRedirect(true)
            .run {
                if (proxy != null) {
                    proxy { option ->
                        val userInfo = proxy.userInfo.split(":")
                        option.type(ProxyProvider.Proxy.HTTP).host(proxy.host).username(userInfo[0]).password { userInfo[1] }.port(proxy.port)
                    }
                } else this
            }

        return webClientBuilder
            .codecs { it.defaultCodecs().maxInMemorySize(DEFAULT_MAX_BODY_SIZE) }
            .clientConnector(ReactorClientHttpConnector(client))
            .build()
    }

    private val DEFAULT_MAX_BODY_SIZE = DataSize.ofMegabytes(20).toBytes().toInt()
    private val DEFAULT_TIMEOUT: Duration = Duration.ofSeconds(30)
}
