package com.rarible.blockchain.scanner.monitoring

import com.rarible.core.telemetry.metrics.Metric.Companion.tag
import io.micrometer.core.instrument.MeterRegistry
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class BlockchainMonitor(
    private val meterRegistry: MeterRegistry
) {
    private val blockchainApiCallsCounter = "blockchain_api_calls"
    private val blockchainApiCallsExecution = "blockchain_api_calls_execution"

    fun onBlockchainCall(blockchain: String, method: String, status: CallStatus = CallStatus.SUCCESS) {
        meterRegistry.counter(
            blockchainApiCallsCounter,
            listOf(
                tag("blockchain", blockchain),
                tag("method", method),
                tag("status", status.value),
            )
        ).increment()
    }

    fun <T> onBlockchainCall(
        blockchain: String,
        method: String,
        monoCall: () -> Mono<T>
    ): Mono<T> {
        val startTimeMs = AtomicLong(0)
        return monoCall()
            .doOnSubscribe {
                startTimeMs.set(System.currentTimeMillis())
            }
            .doOnSuccess {
                onBlockchainCallLatency(startTimeMs.get(), blockchain, method, CallStatus.SUCCESS)
            }
            .doOnError {
                onBlockchainCallLatency(startTimeMs.get(), blockchain, method, CallStatus.ERROR)
            }
    }

    private fun onBlockchainCallLatency(
        startTimeMs: Long,
        blockchain: String,
        method: String,
        status: CallStatus
    ) {
        val endTimeMs = System.currentTimeMillis();
        val executionTime = endTimeMs - startTimeMs;
        meterRegistry.timer(
            blockchainApiCallsExecution,
            listOf(
                tag("blockchain", blockchain),
                tag("method", method),
                tag("status", status.value),
            )
        ).record(executionTime, TimeUnit.MILLISECONDS)
    }

    enum class CallStatus(val value: String) {
        SUCCESS("success"),
        ERROR("error")
    }
}