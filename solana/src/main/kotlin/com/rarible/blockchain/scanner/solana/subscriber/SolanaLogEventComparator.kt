package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord

interface SolanaLogEventComparator : LogEventComparator<SolanaLog, SolanaLogRecord<*>>