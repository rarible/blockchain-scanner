package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import com.rarible.blockchain.scanner.solana.client.dto.ApiResponse
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.SolanaAccountBase64InfoDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaAccountInfoDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBalanceDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto

class MonitoredSolanaApi(
    private val delegate: SolanaApi,
    private val blockchainMonitor: BlockchainMonitor,
    private val blockchain: String = "solana"
) : SolanaApi {

    override suspend fun getFirstAvailableBlock(): ApiResponse<Long> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getFirstAvailableBlock") {
            delegate.getFirstAvailableBlock()
        }
    }

    override suspend fun getLatestSlot(): ApiResponse<Long> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getLatestSlot") {
            delegate.getLatestSlot()
        }
    }

    override suspend fun getBlocks(
        slots: List<Long>,
        details: TransactionDetails
    ): Map<Long, ApiResponse<SolanaBlockDto>> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getBlocks") {
            delegate.getBlocks(slots, details)
        }
    }

    override suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getBlock") {
            delegate.getBlock(slot, details)
        }
    }

    override suspend fun getTransaction(signature: String): ApiResponse<SolanaTransactionDto> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getTransaction") {
            delegate.getTransaction(signature)
        }
    }

    override suspend fun getAccountInfo(address: String): ApiResponse<SolanaAccountInfoDto> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getAccountInfo") {
            delegate.getAccountInfo(address)
        }
    }

    override suspend fun getAccountBase64Info(address: String): ApiResponse<SolanaAccountBase64InfoDto> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getAccountBase64Info") {
            delegate.getAccountBase64Info(address)
        }
    }

    override suspend fun getBalance(address: String): ApiResponse<SolanaBalanceDto> {
        return blockchainMonitor.onBlockchainCallSuspend(blockchain, "getBalance") {
            delegate.getBalance(address)
        }
    }
}
