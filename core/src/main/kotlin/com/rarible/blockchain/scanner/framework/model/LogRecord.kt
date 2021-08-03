package com.rarible.blockchain.scanner.framework.model

interface LogRecord<L : Log, LR : LogRecord<L, LR>> {

    val log: L?

    fun withLog(log: L): LR

}