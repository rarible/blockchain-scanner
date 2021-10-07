package com.rarible.blockchain.scanner.flow.reconciliation

import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.reconciliation.ReconciliationFromProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class FlowReconciliationFromProvider(
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId
): ReconciliationFromProvider {

    override fun initialFrom(): Long = when(chainId) {
        FlowChainId.MAINNET -> 7601063L
        FlowChainId.TESTNET -> 47330084L
        FlowChainId.EMULATOR -> 0L
        else -> throw IllegalArgumentException("Unsupported chain-id : $chainId")
    }
}
