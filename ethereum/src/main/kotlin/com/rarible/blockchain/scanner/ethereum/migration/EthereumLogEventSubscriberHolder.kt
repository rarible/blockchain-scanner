package com.rarible.blockchain.scanner.ethereum.migration

import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import org.springframework.stereotype.Component

@Component
class EthereumLogEventSubscriberHolder(

    val subscribers: List<EthereumLogEventSubscriber>

)