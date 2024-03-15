package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import java.io.IOException

class TestLogEventSubscriberExceptionResolver : LogEventSubscriberExceptionResolver {
    override fun isRetriable(e: Throwable): Boolean {
        return e is IOException
    }
}
