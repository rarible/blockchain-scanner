package com.rarible.blockchain.scanner.monitoring

interface Monitor {

    fun register()

    suspend fun refresh()
}
