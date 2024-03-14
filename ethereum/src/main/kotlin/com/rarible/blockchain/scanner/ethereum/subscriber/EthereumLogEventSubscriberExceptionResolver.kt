package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import io.daonomic.rpc.RpcCodeException
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.io.IOException

@Component
class EthereumLogEventSubscriberExceptionResolver : LogEventSubscriberExceptionResolver {

    override fun shouldInterruptScan(e: Throwable): Boolean {
        return isIOException(e) ||
            isIOException(e.cause) ||
            isIOException(e.cause?.cause)
    }

    private fun isIOException(e: Throwable?): Boolean {
        e ?: return false
        return when (e) {
            is RpcCodeException -> true // Node unavailable
            is WebClientResponseException -> true // Request to external HTTP resource failed
            is IOException -> true // Request to internal HTTP resource failed, e.g. protocol-currency-api
            is DataAccessResourceFailureException -> true // Mongo connection error
            else -> false
        }
    }
}
