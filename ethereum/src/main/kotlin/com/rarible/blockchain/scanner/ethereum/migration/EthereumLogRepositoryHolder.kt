package com.rarible.blockchain.scanner.ethereum.migration

import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import org.springframework.stereotype.Component

@Component
class EthereumLogRepositoryHolder(

    val repositories: List<EthereumLogRepository>

)
