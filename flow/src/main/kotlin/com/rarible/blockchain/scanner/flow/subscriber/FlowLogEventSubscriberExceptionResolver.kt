package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import org.springframework.stereotype.Component

@Component
class FlowLogEventSubscriberExceptionResolver : LogEventSubscriberExceptionResolver {

    override fun shouldInterruptScan(e: Throwable): Boolean {
        return false
    }
}
