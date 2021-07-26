package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.EventData
import org.springframework.stereotype.Component

@Component
class LogEventSubscriberHolder<OL, OB, D : EventData>(

    val subscribers: List<LogEventSubscriber<OL, OB, D>>

)