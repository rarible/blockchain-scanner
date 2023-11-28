package com.rarible.blockchain.scanner.framework.subscriber

interface LogEventSubscriberExceptionResolver {

    fun shouldInterruptScan(e: Throwable): Boolean
}
