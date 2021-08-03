package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log

class LogEvent<L : Log, D : Descriptor>(
    val log: L,
    val descriptor: D
)