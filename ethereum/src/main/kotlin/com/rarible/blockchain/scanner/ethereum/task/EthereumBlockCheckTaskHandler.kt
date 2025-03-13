package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.task.BlockCheckTaskHandler
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class EthereumBlockCheckTaskHandler(
    @Qualifier("ethereumScannerManager") manager: EthereumScannerManager
) : BlockCheckTaskHandler<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, TransactionRecord, EthereumDescriptor, EthereumLogRepository>(
    manager
)
