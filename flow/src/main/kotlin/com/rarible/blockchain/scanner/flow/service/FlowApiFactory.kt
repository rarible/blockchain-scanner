package com.rarible.blockchain.scanner.flow.service

interface FlowApiFactory {
    fun getApi(spork: Spork): AsyncFlowAccessApi
}
