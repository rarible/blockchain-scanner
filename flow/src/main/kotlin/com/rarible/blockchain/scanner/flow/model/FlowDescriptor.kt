package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

class FlowDescriptor(
    override val id: String,
    val events: Set<String>,
    val collection: String,
    val startFrom: Long? = null
): Descriptor
