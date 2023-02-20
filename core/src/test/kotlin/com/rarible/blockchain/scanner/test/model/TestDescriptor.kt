package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

data class TestDescriptor(
    val topic: String,
    val collection: String,
    val contracts: List<String>,
    override val entityType: Class<*>,
    override val groupId: String = topic,
    override val id: String = topic
) : Descriptor
