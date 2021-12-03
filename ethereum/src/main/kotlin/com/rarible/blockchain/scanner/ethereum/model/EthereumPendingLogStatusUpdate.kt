package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Log

class EthereumPendingLogStatusUpdate(
    val logs: List<EthereumPendingLog>,
    val status: Log.Status
)
