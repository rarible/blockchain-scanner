package com.rarible.blockchain.scanner.client

import com.github.michaelbull.retry.RetryAfter
import com.github.michaelbull.retry.context.retryStatus
import com.github.michaelbull.retry.policy.RetryPolicy
import java.time.Duration
import kotlin.coroutines.coroutineContext

fun linearDelay(init: Duration, increment: Duration): RetryPolicy<*> {
    return {
        val attempt = coroutineContext.retryStatus.attempt

        /* sleep = init + increment * attempt */
        val delay = init + increment.multipliedBy(attempt.toLong())

        RetryAfter(delay.toMillis())
    }
}
