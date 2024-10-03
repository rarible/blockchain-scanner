package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Disabled("manual")
internal class FlowApiFactoryImplTest {

    @Test
    fun test() {
        val spork = Spork(
            from = 0,
            nodeUrl = "",
            port = 9000,
            headers = mapOf("x-token" to ""),
        )
        val api = FlowApiFactoryImpl(
            blockchainMonitor = BlockchainMonitor(SimpleMeterRegistry()),
            flowBlockchainScannerProperties = FlowBlockchainScannerProperties(
                sporks = listOf(
                    spork
                )
            )
        ).getApi(spork)

        println(api.getLatestBlockHeader().get())
    }
}
