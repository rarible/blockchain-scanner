package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

class TestDescriptor(
    val topic: String,
    val collection: String,
    val contracts: List<String>
) : Descriptor {
    override val id: String get() = topic
    override val groupId: String get() = topic
}


