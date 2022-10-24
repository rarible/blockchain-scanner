package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.ethereum.domain.Blockchain

interface MetricProperties {
    val blockchain: Blockchain
    val metricRootPath: String
}
