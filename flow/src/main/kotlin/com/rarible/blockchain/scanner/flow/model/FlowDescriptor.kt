package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

data class FlowDescriptor(
    override val id: String,
    override val groupId: String,
    val events: Set<String>,
    val collection: String,
    val startFrom: Long? = null,
    override val entityType: Class<*>,
    override val alias: String? = null
) : Descriptor