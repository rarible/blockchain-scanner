package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log

interface LogEventPostProcessor<L : Log> {

    suspend fun postProcessLogs(logs: List<L>)

}