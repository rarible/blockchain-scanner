package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.model.Descriptor

data class FlowDescriptor(
    override val id: String,
    override val groupId: String,
    val events: Set<String>,
    val address: String,
    // DB collection
    val startFrom: Long? = null,
    override val alias: String? = null,
    override val storage: FlowLogRepository,
) : Descriptor<FlowLogRepository>
