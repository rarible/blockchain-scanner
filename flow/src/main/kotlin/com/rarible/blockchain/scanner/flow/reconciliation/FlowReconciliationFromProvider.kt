package com.rarible.blockchain.scanner.flow.reconciliation

import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.service.SporkService
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.reconciliation.ReconciliationFromProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class FlowReconciliationFromProvider(
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId,
    private val subscribers: List<FlowLogEventSubscriber>,
    private val sporkService: SporkService
) : ReconciliationFromProvider {

    override fun initialFrom(descriptorId: String): Long {
        val sub = subscribers.first { descriptorId == it.getDescriptor().id }
        return sub.getDescriptor().startFrom ?: when(chainId) {
            FlowChainId.MAINNET, FlowChainId.TESTNET -> sporkService.allSporks[chainId]!!.last().from
            FlowChainId.EMULATOR -> 0L
            else -> throw IllegalArgumentException("Unsupported chain-id : $chainId")
        }
    }
}
