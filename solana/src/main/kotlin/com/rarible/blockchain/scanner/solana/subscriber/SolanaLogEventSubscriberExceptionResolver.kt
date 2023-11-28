package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import org.springframework.stereotype.Component

@Component
class SolanaLogEventSubscriberExceptionResolver : LogEventSubscriberExceptionResolver {

    override fun shouldInterruptScan(e: Throwable): Boolean {
        return false
    }
}
