package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.model.EventData

data class TestEventData(
    val minorIndex: Int,
    val blockExtra: String,
    val logExtra: String
) : EventData