package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Descriptor
import io.daonomic.rpc.domain.Word
import scalether.domain.Address

class EthereumDescriptor(
    val ethTopic: Word,
    override val groupId: String,
    override val topic: String,
    val collection: String,
    val contracts: List<Address>
) : Descriptor {

    override val id: String get() = ethTopic.toString()

}
