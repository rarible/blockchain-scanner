package com.rarible.blockchain.scanner.flow.http

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.nftco.flow.sdk.FlowEvent
import com.nftco.flow.sdk.FlowEventPayload
import com.nftco.flow.sdk.FlowEventResult
import com.nftco.flow.sdk.FlowId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.http.model.BlockEvents
import com.rarible.blockchain.scanner.flow.http.model.Event
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.time.delay
import org.bouncycastle.util.encoders.Base64
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.util.DefaultUriBuilderFactory
import java.io.IOException
import java.net.URI
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class FlowHttpClientImpl(
    properties: FlowBlockchainScannerProperties
) : FlowHttpApi {

    private val clientProperties = properties.httpApiClient
    private val transport = HttpClientBuilder.build(clientProperties.proxy)
    private val uriBuilderFactory = DefaultUriBuilderFactory(clientProperties.endpoint.toASCIIString())

    private val mapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        setSerializationInclusion(JsonInclude.Include.NON_NULL)
    }

    override suspend fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> {
        logger.info("Http call eventsByBlockRange: type=$type, range=$range, endpoint=${clientProperties.endpoint}")
        val uri = uriBuilderFactory.builder().run {
            path("/v1/events")
            queryParam("type", type)
            queryParam("start_height", range.first)
            queryParam("end_height", range.last)
            build()
        }
        return getResult(uri, Array<BlockEvents>::class.java).map { convert(it) }.asFlow()
    }

    private suspend fun <R> getResult(uri: URI, type: Class<R>): R {
        var attempt = clientProperties.attempts
        var lastOperationResult: OperationResult.Error<R>?
        do {
            when (val result = getOperationResult(uri, type)) {
                is OperationResult.Success -> return result.value
                is OperationResult.Error -> {
                    lastOperationResult = result
                    attempt -= 1
                    delay(clientProperties.delay)
                }
            }
        } while (attempt > 0)

        throw IOException(lastOperationResult?.toString() ?: "Unknown error, $uri")
    }

    private suspend fun <R> getOperationResult(uri: URI, type: Class<R>): OperationResult<R> {
        return transport.get().uri(uri).exchangeToMono {
            mono {
                val code = it.statusCode()
                val body = it.awaitBody<ByteArray>()
                when (code) {
                    HttpStatus.OK -> OperationResult.Success<R>(mapper.readValue(body, type))
                    else ->  OperationResult.Error("Flow Api exception: code=$code, body=${String(body)}, url=$uri")
                }
            }
        }.awaitFirst()
    }

    private fun convert(blockEvents: BlockEvents): FlowEventResult {
        return FlowEventResult(
            blockId = FlowId(blockEvents.blockId),
            blockHeight = blockEvents.blockHeight.toLong(),
            blockTimestamp = LocalDateTime.ofInstant(blockEvents.blockTimestamp, ZoneOffset.UTC),
            events = blockEvents.events.map { convert(it) }
        )
    }

    private fun convert(event: Event): FlowEvent {
        return FlowEvent(
            type = event.type,
            transactionId = FlowId(event.transactionId),
            transactionIndex = event.transactionIndex.toInt(),
            eventIndex = event.eventIndex.toInt(),
            payload = FlowEventPayload(Base64.decode(event.payload))
        )
    }

    private sealed class OperationResult<T> {
        data class Success<T>(val value: T) : OperationResult<T>()
        data class Error<T>(val message: String) : OperationResult<T>()
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(FlowHttpClientImpl::class.java)
    }
}

