package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.core.telemetry.metrics.AbstractMetrics
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class HederaClientMetrics(
    meterRegistry: MeterRegistry
) : AbstractMetrics(meterRegistry) {
    fun recordLatency(uri: String, start: Instant, method: String, status: String): Duration {
        val latency = Duration.between(start, Instant.now())
        record(
            REQUEST_LATENCY,
            latency,
            tag(URI_TAG, uri),
            tag(METHOD_TAG, method),
            tag(STATUS_TAG, status)
        )
        return latency
    }

    fun recordParseError(uri: String, method: String) {
        increment(RESPONSE_PARSE_ERROR, tag(URI_TAG, uri), tag(METHOD_TAG, method))
    }

    companion object {
        const val REQUEST_LATENCY = "hedera_client_request_latency_seconds"
        const val RESPONSE_PARSE_ERROR = "hedera_client_response_parse_error_count"
        const val URI_TAG = "uri"
        const val METHOD_TAG = "method"
        const val STATUS_TAG = "status"
    }
}
