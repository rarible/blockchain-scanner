package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

abstract class SolanaLogRecord(
    val log: SolanaLog
) : LogRecord