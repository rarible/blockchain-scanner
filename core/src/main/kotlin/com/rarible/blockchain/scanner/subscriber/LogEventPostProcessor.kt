package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log

interface LogEventPostProcessor<L : Log> {

    //todo Серега, а вот мы не можем использовать для notification в кафка эту штуку? А не отдельный еще LogEventListener? 2 сущности есть, которые слушают события. думаю, можно унифицировать
    suspend fun postProcessLogs(logs: List<L>)

}