package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.model.Block
import org.springframework.data.annotation.Id

class EthereumBlock(
    @Id
    override val id: Long,

    override val hash: String,
    override val number: Long,
    override val timestamp: Long
) : Block