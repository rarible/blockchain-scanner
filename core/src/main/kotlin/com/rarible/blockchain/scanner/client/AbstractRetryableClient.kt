package com.rarible.blockchain.scanner.client

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.StopRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.time.delay
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.cancellation.CancellationException

abstract class AbstractRetryableClient(
    private val retryPolicy: ClientRetryPolicyProperties
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val delay = retryPolicy.delay
    private val increment = retryPolicy.increment
    private val attempts = retryPolicy.attempts

    private val retryExceptionFilter: RetryPolicy<Throwable> = {
        if (isRetryableException(reason)) {
            ContinueRetrying
        } else {
            StopRetrying
        }
    }

    protected fun <T> Flow<T>.wrapWithRetry(method: String = "", vararg args: Any): Flow<T> {
        val currentDelay = AtomicReference(delay)
        return this
            .retryWhen { e, currentAttempt ->
                if (isRetryableException(e)) {
                    // currentAttempt start from 0
                    if (currentAttempt + 1 >= attempts) {
                        logRetryFail(method, e, *args)
                        false
                    } else {
                        val sleep = currentDelay.get()
                        delay(sleep)
                        currentDelay.set(sleep + increment.multipliedBy(currentAttempt))
                        true
                    }
                } else {
                    false
                }
            }
    }

    protected fun <T> Mono<T>.wrapWithRetry(method: String = "", vararg args: Any): Mono<T> {
        val currentDelay = AtomicReference(delay)
        return this
            .retryWhen(
                Retry.max(attempts.toLong() - 1).filter { e ->
                    val shouldRetry = isRetryableException(e)
                    if (shouldRetry) {
                        logger.info("Will retry $method because of ${e.message}")
                    }
                    shouldRetry
                }.doBeforeRetryAsync {
                    val sleep = currentDelay.get()
                    currentDelay.set(sleep + increment)
                    logger.info("Will sleep for $sleep")
                    Mono.delay(sleep).then()
                }.onRetryExhaustedThrow { _, signal ->
                    logRetryFail(method, signal.failure(), *args)
                    signal.failure()
                }
            )
    }

    protected suspend fun <T> wrapWithRetry(method: String, vararg args: Any, clientCall: suspend () -> T): T {
        try {
            return if (attempts > 0) {
                retry(retryExceptionFilter + limitAttempts(attempts) + linearDelay(delay, increment)) {
                    clientCall.invoke()
                }
            } else {
                clientCall.invoke()
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logRetryFail(method, e, *args)
            throw e
        }
    }

    private fun isRetryableException(e: Throwable): Boolean {
        return e !is CancellationException &&
            e !is NonRetryableBlockchainClientException &&
            e !is Error
    }

    private fun logRetryFail(method: String, e: Throwable, vararg args: Any) {
        logger.warn(
            "Unable to perform BlockchainClient operation '{}' with params [{}] after {} attempts: {}",
            method, args.contentToString(), retryPolicy.attempts, e.message, e
        )
    }
}
