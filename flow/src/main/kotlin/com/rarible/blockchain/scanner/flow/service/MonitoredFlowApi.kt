package com.rarible.blockchain.scanner.flow.service

import com.google.protobuf.ByteString
import com.nftco.flow.sdk.FlowAccount
import com.nftco.flow.sdk.FlowAddress
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowBlockHeader
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowCollection
import com.nftco.flow.sdk.FlowEventResult
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowScript
import com.nftco.flow.sdk.FlowScriptResponse
import com.nftco.flow.sdk.FlowSnapshot
import com.nftco.flow.sdk.FlowTransaction
import com.nftco.flow.sdk.FlowTransactionResult
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import java.io.Closeable
import java.util.concurrent.CompletableFuture

class MonitoredFlowApi(
    private val delegate: AsyncFlowAccessApi,
    private val closeable: Closeable,
    private val blockchainMonitor: BlockchainMonitor,
    private val blockchain: String = "flow",
) : AsyncFlowAccessApi, Closeable {
    override fun executeScriptAtBlockHeight(
        script: FlowScript,
        height: Long,
        arguments: Iterable<ByteString>
    ): CompletableFuture<FlowScriptResponse> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "executeScriptAtBlockHeight") {
            delegate.executeScriptAtBlockHeight(script, height, arguments)
        }
    }

    override fun executeScriptAtBlockId(
        script: FlowScript,
        blockId: FlowId,
        arguments: Iterable<ByteString>
    ): CompletableFuture<FlowScriptResponse> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "executeScriptAtBlockId") {
            delegate.executeScriptAtBlockId(script, blockId, arguments)
        }
    }

    override fun executeScriptAtLatestBlock(
        script: FlowScript,
        arguments: Iterable<ByteString>
    ): CompletableFuture<FlowScriptResponse> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "executeScriptAtLatestBlock") {
            delegate.executeScriptAtLatestBlock(script, arguments)
        }
    }

    override fun getAccountAtLatestBlock(addresss: FlowAddress): CompletableFuture<FlowAccount?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getAccountAtLatestBlock") {
            delegate.getAccountAtLatestBlock(addresss)
        }
    }

    override fun getAccountByAddress(addresss: FlowAddress): CompletableFuture<FlowAccount?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getAccountByAddress") {
            delegate.getAccountByAddress(addresss)
        }
    }

    override fun getAccountByBlockHeight(addresss: FlowAddress, height: Long): CompletableFuture<FlowAccount?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getAccountByBlockHeight") {
            delegate.getAccountByBlockHeight(addresss, height)
        }
    }

    override fun getBlockByHeight(height: Long): CompletableFuture<FlowBlock?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getBlockByHeight") {
            delegate.getBlockByHeight(height)
        }
    }

    override fun getBlockById(id: FlowId): CompletableFuture<FlowBlock?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getBlockById") {
            delegate.getBlockById(id)
        }
    }

    override fun getBlockHeaderByHeight(height: Long): CompletableFuture<FlowBlockHeader?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getBlockHeaderByHeight") {
            delegate.getBlockHeaderByHeight(height)
        }
    }

    override fun getBlockHeaderById(id: FlowId): CompletableFuture<FlowBlockHeader?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getBlockHeaderById") {
            delegate.getBlockHeaderById(id)
        }
    }

    override fun getCollectionById(id: FlowId): CompletableFuture<FlowCollection?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getCollectionById") {
            delegate.getCollectionById(id)
        }
    }

    override fun getEventsForBlockIds(type: String, ids: Set<FlowId>): CompletableFuture<List<FlowEventResult>> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getEventsForBlockIds") {
            delegate.getEventsForBlockIds(type, ids)
        }
    }

    override fun getEventsForHeightRange(
        type: String,
        range: ClosedRange<Long>
    ): CompletableFuture<List<FlowEventResult>> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getEventsForHeightRange") {
            delegate.getEventsForHeightRange(type, range)
        }
    }

    override fun getLatestBlock(sealed: Boolean): CompletableFuture<FlowBlock> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getLatestBlock") {
            delegate.getLatestBlock(sealed)
        }
    }

    override fun getLatestBlockHeader(): CompletableFuture<FlowBlockHeader> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getLatestBlockHeader") {
            delegate.getLatestBlockHeader()
        }
    }

    override fun getLatestProtocolStateSnapshot(): CompletableFuture<FlowSnapshot> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getLatestProtocolStateSnapshot") {
            delegate.getLatestProtocolStateSnapshot()
        }
    }

    override fun getNetworkParameters(): CompletableFuture<FlowChainId> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getNetworkParameters") {
            delegate.getNetworkParameters()
        }
    }

    override fun getTransactionById(id: FlowId): CompletableFuture<FlowTransaction?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getTransactionById") {
            delegate.getTransactionById(id)
        }
    }

    override fun getTransactionResultById(id: FlowId): CompletableFuture<FlowTransactionResult?> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getTransactionResultById") {
            delegate.getTransactionResultById(id)
        }
    }

    override fun getTransactionResultsByBlockId(id: FlowId): CompletableFuture<List<FlowTransactionResult>> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "getTransactionResultsByBlockId") {
            delegate.getTransactionResultsByBlockId(id)
        }
    }

    override fun ping(): CompletableFuture<Unit> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "ping") {
            delegate.ping()
        }
    }

    override fun sendTransaction(transaction: FlowTransaction): CompletableFuture<FlowId> {
        return blockchainMonitor.onBlockchainCallWithFuture(blockchain, "sendTransaction") {
            delegate.sendTransaction(transaction)
        }
    }

    override fun close() {
        closeable.close()
    }
}
