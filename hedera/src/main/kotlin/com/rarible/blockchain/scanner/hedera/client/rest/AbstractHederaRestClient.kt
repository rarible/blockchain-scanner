package com.rarible.blockchain.scanner.hedera.client.rest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import org.springframework.web.reactive.function.client.createExceptionAndAwait
import org.springframework.web.util.UriBuilder
import java.time.Duration
import java.time.Instant

abstract class AbstractHederaRestClient(
    protected val webClient: WebClient,
    protected val maxErrorBodyLogLength: Int = 10000,
    private val metrics: HederaClientMetrics,
    private val slowRequestLatency: Duration = Duration.ofSeconds(2)
) {
    protected val logger: Logger = LoggerFactory.getLogger(javaClass)

    protected suspend inline fun <reified T : Any> get(
        uri: String,
        vararg params: Any,
        errorProcessor: ErrorProcessor<T> = defaultErrorProcessor,
        noinline uriCustomizer: UriBuilder.() -> UriBuilder = { this },
    ): T {
        val method = HttpMethod.GET.name
        val requestSpec = webClient.get()
            .uri { uriBuilder ->
                if (uri.contains("?")) {
                    // If URI contains query parameters, use it as is
                    uriBuilder.path(uri.substringBefore("?"))
                        .query(uri.substringAfter("?"))
                        .build()
                } else {
                    val builder = uriBuilder.path(uri)
                    if (params.isNotEmpty()) {
                        builder.build(*params)
                    } else {
                        uriCustomizer.invoke(builder).build()
                    }
                }
            }
            .accept(MediaType.ALL)

        return performRequest(
            requestSpec = requestSpec,
            requestType = uri,
            method = method,
            typeDebugInfo = { T::class.java.simpleName },
            resultParser = { responseBody ->
                WebClientFactory.webClientMapper.readValue(responseBody, T::class.java)
            },
            errorProcessor = errorProcessor
        )
    }

    protected suspend fun <T : Any> performRequest(
        requestSpec: WebClient.RequestHeadersSpec<*>,
        requestType: String,
        method: String,
        typeDebugInfo: () -> String,
        resultParser: (String) -> T,
        errorProcessor: ErrorProcessor<T>
    ): T {
        val start = Instant.now()
        return requestSpec
            .awaitExchange { response ->
                val responseBody = response.awaitBody<String>()
                val latency = metrics.recordLatency(requestType, start, method, response.statusCode().value().toString())
                if (latency > slowRequestLatency) {
                    logger.warn("Slow request $latency: method: $method, type: $requestType")
                }
                if (response.statusCode().is2xxSuccessful) {
                    try {
                        resultParser(responseBody)
                    } catch (e: Exception) {
                        metrics.recordParseError(requestType, method)
                        val truncatedResponseBody = responseBody.truncate(maxErrorBodyLogLength)
                        throw DecodingException("Failed to parse ${typeDebugInfo()} from JSON $truncatedResponseBody", e)
                    }
                } else {
                    errorProcessor.processError(response, responseBody, method, requestType)
                }
            }
    }

    protected interface ErrorProcessor<out T : Any> {
        suspend fun processError(
            response: ClientResponse,
            responseBody: String,
            method: String,
            requestType: String
        ): T
    }

    protected val defaultErrorProcessor = object : ErrorProcessor<Nothing> {
        override suspend fun processError(
            response: ClientResponse,
            responseBody: String,
            method: String,
            requestType: String
        ): Nothing {
            val truncatedResponseBody = responseBody.truncate(maxErrorBodyLogLength)
            logger.error("Failed hedera request for $method $requestType. Response: ${response.statusCode()} [$truncatedResponseBody]")
            throw response.createExceptionAndAwait()
        }
    }

    private fun String.truncate(maxLength: Int) =
        if (length > maxLength) substring(0, maxLength) + "... (truncated at $maxLength, total size $length)"
        else this
}
