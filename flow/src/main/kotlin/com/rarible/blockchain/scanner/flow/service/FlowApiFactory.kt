package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.AsyncFlowAccessApi

interface FlowApiFactory {
    fun getApi(spork: Spork): AsyncFlowAccessApi
}
