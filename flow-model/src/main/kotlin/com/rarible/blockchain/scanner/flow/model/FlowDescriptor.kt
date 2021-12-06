package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

data class FlowDescriptor(
    override val id: String,
    override val groupId: String,
    override val topic: String,
    val events: Set<String>,
    val collection: String,
    val startFrom: Long? = null
) : Descriptor