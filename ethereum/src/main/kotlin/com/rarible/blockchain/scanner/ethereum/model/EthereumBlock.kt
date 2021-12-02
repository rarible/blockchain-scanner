package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.data.annotation.Id

data class EthereumBlock(
    @Id
    override val id: Long,

    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long
) : Block