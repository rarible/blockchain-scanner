package com.rarible.blockchain.scanner.flow.migration

import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import org.springframework.stereotype.Component

@Component
class FlowLogEventSubscriberHolder(
    val subscribers: List<FlowLogEventSubscriber>,
)
