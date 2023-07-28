package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter

interface EthereumLogEventFilter : LogEventFilter<EthereumLogRecord, EthereumDescriptor>
