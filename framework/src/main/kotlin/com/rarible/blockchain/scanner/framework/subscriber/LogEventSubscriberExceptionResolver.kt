package com.rarible.blockchain.scanner.framework.subscriber

interface LogEventSubscriberExceptionResolver {

    fun shouldInterruptScan(e: Throwable): Boolean {
      val scannedThrowables = hashSetOf<Throwable>()
      return generateSequence(e) { t -> t.cause?.takeIf { scannedThrowables.add(t) } }
        .any { isRetriable(it) }
    }

    fun isRetriable(e: Throwable): Boolean
}
