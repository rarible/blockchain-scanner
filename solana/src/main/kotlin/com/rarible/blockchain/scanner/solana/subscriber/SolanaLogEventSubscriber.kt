package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.model.SolanaLogStorage

interface SolanaLogEventSubscriber :
    LogEventSubscriber<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaLogRecord, SolanaDescriptor, SolanaLogStorage>
