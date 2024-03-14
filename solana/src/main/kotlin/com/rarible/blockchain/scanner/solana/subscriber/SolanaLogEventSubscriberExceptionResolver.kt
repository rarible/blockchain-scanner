package com.rarible.blockchain.scanner.solana.subscriber

import com.mongodb.MongoSocketException
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientException
import java.io.IOException

@Component
class SolanaLogEventSubscriberExceptionResolver : LogEventSubscriberExceptionResolver {

    override fun isRetriable(e: Throwable): Boolean = when (e) {
        is WebClientException -> true // Request to external HTTP resource failed
        is IOException -> true // Request to internal HTTP resource failed, e.g. protocol-currency-api
        is DataAccessResourceFailureException -> true // Mongo connection error
        is MongoSocketException -> true // Mongo connection error as well
        else -> false
    }
}
