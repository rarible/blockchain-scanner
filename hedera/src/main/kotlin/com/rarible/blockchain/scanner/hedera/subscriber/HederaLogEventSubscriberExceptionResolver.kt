package com.rarible.blockchain.scanner.hedera.subscriber

import com.mongodb.MongoSocketException
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientException
import java.io.IOException

@Component
class HederaLogEventSubscriberExceptionResolver : LogEventSubscriberExceptionResolver {

    override fun isRetriable(e: Throwable): Boolean = when (e) {
        is WebClientException, // Request to an external or internal HTTP resource failed
        is IOException, // Generic I/O problem should be retriable
        is DataAccessResourceFailureException, // Mongo connection error
        is MongoSocketException -> true // Mongo connection error as well
        else -> false
    }
}
