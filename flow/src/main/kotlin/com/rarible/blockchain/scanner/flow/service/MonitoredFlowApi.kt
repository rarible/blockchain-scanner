package com.rarible.blockchain.scanner.flow.service

import com.google.protobuf.ByteString
import com.nftco.flow.sdk.AsyncFlowAccessApi
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
        blockchainMonitor.onBlockchainCall(blockchain, "executeScriptAtBlockHeight")
        return delegate.executeScriptAtBlockHeight(script, height, arguments)
    }

    override fun executeScriptAtBlockId(
        script: FlowScript,
        blockId: FlowId,
        arguments: Iterable<ByteString>
    ): CompletableFuture<FlowScriptResponse> {
        blockchainMonitor.onBlockchainCall(blockchain, "executeScriptAtBlockId")
        return delegate.executeScriptAtBlockId(script, blockId, arguments)
    }

    override fun executeScriptAtLatestBlock(
        script: FlowScript,
        arguments: Iterable<ByteString>
    ): CompletableFuture<FlowScriptResponse> {
        blockchainMonitor.onBlockchainCall(blockchain, "executeScriptAtLatestBlock")
        return delegate.executeScriptAtLatestBlock(script, arguments)
    }

    override fun getAccountAtLatestBlock(addresss: FlowAddress): CompletableFuture<FlowAccount?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getAccountAtLatestBlock")
        return delegate.getAccountAtLatestBlock(addresss)
    }

    override fun getAccountByAddress(addresss: FlowAddress): CompletableFuture<FlowAccount?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getAccountByAddress")
        return delegate.getAccountByAddress(addresss)
    }

    override fun getAccountByBlockHeight(addresss: FlowAddress, height: Long): CompletableFuture<FlowAccount?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getAccountByBlockHeight")
        return delegate.getAccountByBlockHeight(addresss, height)
    }

    override fun getBlockByHeight(height: Long): CompletableFuture<FlowBlock?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getBlockByHeight")
        return delegate.getBlockByHeight(height)
    }

    override fun getBlockById(id: FlowId): CompletableFuture<FlowBlock?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getBlockById")
        return delegate.getBlockById(id)
    }

    override fun getBlockHeaderByHeight(height: Long): CompletableFuture<FlowBlockHeader?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getBlockHeaderByHeight")
        return delegate.getBlockHeaderByHeight(height)
    }

    override fun getBlockHeaderById(id: FlowId): CompletableFuture<FlowBlockHeader?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getBlockHeaderById")
        return delegate.getBlockHeaderById(id)
    }

    override fun getCollectionById(id: FlowId): CompletableFuture<FlowCollection?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getCollectionById")
        return delegate.getCollectionById(id)
    }

    override fun getEventsForBlockIds(type: String, ids: Set<FlowId>): CompletableFuture<List<FlowEventResult>> {
        blockchainMonitor.onBlockchainCall(blockchain, "getEventsForBlockIds")
        return delegate.getEventsForBlockIds(type, ids)
    }

    override fun getEventsForHeightRange(
        type: String,
        range: ClosedRange<Long>
    ): CompletableFuture<List<FlowEventResult>> {
        blockchainMonitor.onBlockchainCall(blockchain, "getEventsForHeightRange")
        return delegate.getEventsForHeightRange(type, range)
    }

    override fun getLatestBlock(sealed: Boolean): CompletableFuture<FlowBlock> {
        blockchainMonitor.onBlockchainCall(blockchain, "getLatestBlock")
        return delegate.getLatestBlock(sealed)
    }

    override fun getLatestBlockHeader(): CompletableFuture<FlowBlockHeader> {
        blockchainMonitor.onBlockchainCall(blockchain, "getLatestBlockHeader")
        return delegate.getLatestBlockHeader()
    }

    override fun getLatestProtocolStateSnapshot(): CompletableFuture<FlowSnapshot> {
        blockchainMonitor.onBlockchainCall(blockchain, "getLatestProtocolStateSnapshot")
        return delegate.getLatestProtocolStateSnapshot()
    }

    override fun getNetworkParameters(): CompletableFuture<FlowChainId> {
        blockchainMonitor.onBlockchainCall(blockchain, "getNetworkParameters")
        return delegate.getNetworkParameters()
    }

    override fun getTransactionById(id: FlowId): CompletableFuture<FlowTransaction?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getTransactionById")
        return delegate.getTransactionById(id)
    }

    override fun getTransactionResultById(id: FlowId): CompletableFuture<FlowTransactionResult?> {
        blockchainMonitor.onBlockchainCall(blockchain, "getTransactionResultById")
        return delegate.getTransactionResultById(id)
    }

    override fun ping(): CompletableFuture<Unit> {
        blockchainMonitor.onBlockchainCall(blockchain, "ping")
        return delegate.ping()
    }

    override fun sendTransaction(transaction: FlowTransaction): CompletableFuture<FlowId> {
        blockchainMonitor.onBlockchainCall(blockchain, "sendTransaction")
        return delegate.sendTransaction(transaction)
    }

    override fun close() {
        closeable.close()
    }
}