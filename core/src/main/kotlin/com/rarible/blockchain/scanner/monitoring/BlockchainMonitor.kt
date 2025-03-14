package com.rarible.blockchain.scanner.monitoring

import com.rarible.core.telemetry.metrics.Metric.Companion.tag
import io.micrometer.core.instrument.MeterRegistry
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class BlockchainMonitor(
    private val meterRegistry: MeterRegistry
) {
    private val blockchainApiCallsCounter = "blockchain_api_calls"

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

    fun <T> onBlockchainCallWithFuture(
        blockchain: String,
        method: String,
        futureCall: () -> CompletableFuture<T>
    ): CompletableFuture<T> {
        val startTimeMs = AtomicLong(System.currentTimeMillis())
        return futureCall()
            .whenComplete { _, error ->
                val status = if (error == null) CallStatus.SUCCESS else CallStatus.ERROR
                onBlockchainCallLatency(startTimeMs.get(), blockchain, method, status)
            }
    }

    private fun onBlockchainCallLatency(
        startTimeMs: Long,
        blockchain: String,
        method: String,
        status: CallStatus,
        node: CallNode = CallNode.MAIN
    ) {
        val endTimeMs = System.currentTimeMillis()
        val executionTime = endTimeMs - startTimeMs
        meterRegistry.timer(
            blockchainApiCallsCounter,
            listOf(
                tag("blockchain", blockchain),
                tag("method", method),
                tag("status", status.value),
                tag("node", node.value),
            )
        ).record(executionTime, TimeUnit.MILLISECONDS)
    }

    suspend fun <T> onBlockchainCallSuspend(
        blockchain: String,
        method: String,
        node: CallNode,
        call: suspend () -> T
    ): T {
        val startTimeMs = System.currentTimeMillis()
        return try {
            call().also {
                onBlockchainCallLatency(startTimeMs, blockchain, method, CallStatus.SUCCESS, node)
            }
        } catch (e: Exception) {
            onBlockchainCallLatency(startTimeMs, blockchain, method, CallStatus.ERROR, node)
            throw e
        }
    }

    enum class CallStatus(val value: String) {
        SUCCESS("success"),
        ERROR("error")
    }

    enum class CallNode(val value: String) {
        MAIN("main"),
        RECONCILIATION("reconciliation")
    }
}
