package com.rarible.blockchain.scanner.solana.migration

import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import org.springframework.stereotype.Component

@Component
class SolanaLogEventSubscriberHolder(

    val subscribers: List<SolanaLogEventSubscriber>

)
