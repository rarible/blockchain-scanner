package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Descriptor
import io.daonomic.rpc.domain.Word
import scalether.domain.Address

class EthereumDescriptor(
    val ethTopic: Word,
    override val groupId: String,
    val collection: String,
    val contracts: List<Address>,
    override val entityType: Class<*>
) : Descriptor {

    override val id: String get() = ethTopic.toString()

}
