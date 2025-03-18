package com.rarible.blockchain.scanner.ethereum.client.hyper

import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import scalether.domain.Address
import scalether.domain.response.Block
import scalether.domain.response.Transaction
import scalether.java.Lists
import java.math.BigInteger

class HyperBlockArchiverAdapter(
    private val hyperBlockArchiver: HyperBlockArchiver,
) {
    suspend fun getBlock(blockNumber: BigInteger): Block<Transaction> {
        return convert(hyperBlockArchiver.downloadBlock(blockNumber))
    }

    private fun convert(hyperBlock: HyperBlock): Block<Transaction> {
        val blockHeader = hyperBlock.block.reth115.header
        val header = hyperBlock.block.reth115.header.header
        val blockBody = hyperBlock.block.reth115.body
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
