package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor

class TestLogEventDescriptor(
    override val topic: String,
    val collection: String,
    val contracts: List<String>
) : LogEventDescriptor
