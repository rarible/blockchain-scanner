package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.test.TestSolanaScannerConfiguration
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Disabled("for manual run")
internal class SolanaMetaClientMt {
    private val client = SolanaMetaClient(
        SolanaHttpRpcApi(
            urls = listOf(TestSolanaScannerConfiguration.MAIN_NET_BETA),
            timeoutMillis = 30000
        ),
    )

    @Test
    fun getAccountInfo() = runBlocking<Unit> {
        val result = client.getMetadata("EEqRRJkj9qoTVkqRsdx1wx53tDyfCNdpHrU6waxDHoAZ")!!

        assertThat(result.name).isEqualTo("Captured")
        assertThat(result.symbol).isEqualTo("MUTANT")
        assertThat(result.updateAuthority).isEqualTo("mmonhZY7bK2459JMivLbrb3ifHREKrDmydbbpueX6jG")
        assertThat(result.uri)
            .isEqualTo("https://shdw-drive.genesysgo.net/893AmBr2P9NVydpWc2TAkR3prwBtWMZH2A8RniDmduhn/captured.json")
        assertThat(result.mint).isEqualTo("EEqRRJkj9qoTVkqRsdx1wx53tDyfCNdpHrU6waxDHoAZ")
        assertThat(result.additionalMetadata).hasSize(9)
        assertThat(result.additionalMetadata[0]).isEqualTo(listOf("@Level", "1"))
    }
}
