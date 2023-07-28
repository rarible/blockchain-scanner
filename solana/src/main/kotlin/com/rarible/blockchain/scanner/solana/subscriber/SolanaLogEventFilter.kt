package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord

interface SolanaLogEventFilter : LogEventFilter<SolanaLogRecord, SolanaDescriptor>
