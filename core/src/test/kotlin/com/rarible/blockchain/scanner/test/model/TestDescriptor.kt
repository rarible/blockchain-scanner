package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

class TestDescriptor(
    override val id: String,
    val collection: String,
    val contracts: List<String>,
    override val entityType: Class<*>
) : Descriptor {
    override val groupId: String get() = id
}


