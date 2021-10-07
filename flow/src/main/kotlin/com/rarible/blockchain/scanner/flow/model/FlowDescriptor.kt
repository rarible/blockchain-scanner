package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

class FlowDescriptor(
    val event: String,
    val collection: String
): Descriptor {

    override val id: String = event
}
