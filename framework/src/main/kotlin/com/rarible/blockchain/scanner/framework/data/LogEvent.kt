package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

class LogEvent<L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    val record: R,
    val descriptor: D
)
