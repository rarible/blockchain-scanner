package com.rarible.blockchain.scanner.hedera.client.rest

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.http.codec.json.Jackson2JsonDecoder
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient

object WebClientFactory {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    val webClientMapper: ObjectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    fun createClient(endpoint: String, headers: Map<String, String>): WebClient.Builder {
        logger.info("Creating http client for $endpoint, headers: $headers")
        val httpClient = HttpClient.create()
        val strategies = ExchangeStrategies
            .builder()
            .codecs { configurer: ClientCodecConfigurer ->
                configurer.defaultCodecs()
                    .jackson2JsonEncoder(Jackson2JsonEncoder(webClientMapper, MediaType.APPLICATION_JSON))
                configurer.defaultCodecs()
                    .jackson2JsonDecoder(Jackson2JsonDecoder(webClientMapper, MediaType.APPLICATION_JSON))
            }.build()

        val webClient = WebClient.builder()
            .exchangeStrategies(strategies)
            .clientConnector(ReactorClientHttpConnector(httpClient))

        val finalHeaders = HashMap(headers)

        webClient.defaultHeaders {
            finalHeaders.forEach { header ->
                it.add(header.key, header.value)
            }
        }
        return webClient.baseUrl(endpoint)
    }
}
