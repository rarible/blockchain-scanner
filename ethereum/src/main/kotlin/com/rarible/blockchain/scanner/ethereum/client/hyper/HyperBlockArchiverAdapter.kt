package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.ethereum.client.hyper.Block as HyperBlock
import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import scalether.domain.Address
import scalether.domain.response.Block
import scalether.domain.response.Log
import scalether.domain.response.Transaction
import scalether.java.Lists
import java.math.BigInteger

class HyperBlockArchiverAdapter(
    private val hyperBlockArchiver: CachedHyperBlockArchiver,
) {
    suspend fun getBlock(blockNumber: BigInteger): Block<Transaction> {
        return convert(hyperBlockArchiver.downloadBlock(blockNumber).block)
    }

    suspend fun getLogsByBlockRange(fromBlock: BigInteger, toBlock: BigInteger): List<Log> {
        val range = LongRange(fromBlock.toLong(), toBlock.toLong()).map { BigInteger.valueOf(it) }
        return hyperBlockArchiver.downloadBlocks(range).flatMap { hyperBlock ->
            val ethBlock = convert(hyperBlock.block)
            convert(ethBlock, hyperBlock.receipts)
        }
    }

    private fun convert(block: Block<Transaction>, receipts: List<Receipt>): List<Log> {
        val transactions = Lists.toJava(block.transactions())
        var logIndex = 0L
        return receipts.mapIndexed { receiptIndex, receipt ->
            val transaction = transactions[receiptIndex]
            receipt.logs.map { log ->
                Log(
                    BigInteger.valueOf(logIndex++),
                    BigInteger.valueOf(receiptIndex.toLong()),
                    transaction.hash(),
                    block.hash(),
                    block.number(),
                    Address.apply(log.address),
                    Binary.apply(log.data.data),
                    false, // removed
                    Lists.toScala(log.data.topics.map { Word.apply(it) }),
                    Binary.empty().prefixed() // type
                )
            }
        }.flatten()
    }

    private fun convert(hyperBlock: HyperBlock): Block<Transaction> {
        val blockHeader = hyperBlock.reth115.header
        val header = hyperBlock.reth115.header.header
        val blockBody = hyperBlock.reth115.body
        val blockNumber = BigInteger(header.number)

        val transactions = blockBody.transactions.mapIndexed { index, tx ->
            convertTransaction(
                hyperTx = tx,
                blockNumber = blockNumber,
                blockHash = Word.apply(blockHeader.hash),
                txIndex = BigInteger.valueOf(index.toLong()))
        }
        return Block(
            blockNumber,
            Word.apply(blockHeader.hash),
            Word.apply(header.parentHash),
            Binary.apply(header.nonce).prefixed(),
            Binary.apply(header.sha3Uncles).prefixed(),
            Binary.apply(header.logsBloom).prefixed(),
            Binary.apply(header.transactionsRoot).prefixed(),
            Binary.apply(header.stateRoot).prefixed(),
            Address.apply(header.miner),
            BigInteger(header.difficulty),
            BigInteger(header.difficulty),
            Binary.apply(header.extraData),
            BigInteger.ZERO,
            BigInteger(header.gasLimit),
            BigInteger(header.gasUsed),
            Lists.toScala(transactions),
            BigInteger(header.timestamp),
        )
    }

    private fun convertTransaction(
        hyperTx: com.rarible.blockchain.scanner.ethereum.client.hyper.Transaction,
        blockNumber: BigInteger,
        blockHash: Word,
        txIndex: BigInteger
    ): Transaction {
        val hash = Word.apply(hyperTx.signature.first())
        val commonTx = hyperTx.transaction.getCommonTransaction()

        val nonce = BigInteger(commonTx.nonce)
        val to = Address.apply(commonTx.to)
        val value = BigInteger(commonTx.value)
        val gas = BigInteger(commonTx.gas)
        val gasPrice = BigInteger(commonTx.gasPrice)
        val input = Binary.apply(commonTx.input)
        val creates = Address.ZERO()
        return Transaction(
            hash,
            nonce,
            blockHash,
            blockNumber,
            creates,
            txIndex,
            Address.ZERO(), // from
            to,
            value,
            gasPrice,
            gas,
            input
        )
    }
}
