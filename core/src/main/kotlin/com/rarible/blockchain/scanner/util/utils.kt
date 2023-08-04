package com.rarible.blockchain.scanner.util

import org.slf4j.Logger
import reactor.core.publisher.Mono
import reactor.util.retry.Retry

fun getLogTopicPrefix(environment: String, service: String, blockchain: String, type: String): String {
    return "protocol.$environment.$blockchain.blockchain-scanner.$service.$type"
}

fun <T> Mono<T>.subscribeWithRetry(logger: Logger) = this.doOnError {
    logger.warn("Scanner stopped with error. Will restart", it)
}
    .retryWhen(Retry.indefinitely())
    .subscribe({}, { logger.error("Scanner stopped.", it) })
