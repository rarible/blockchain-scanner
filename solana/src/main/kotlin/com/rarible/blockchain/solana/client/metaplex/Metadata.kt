package com.rarible.blockchain.solana.client.metaplex

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

sealed class MetadataInstruction

class Creator(
    val address: ByteArray,
    val verified: Boolean,
    // In percentages, NOT basis points ;) Watch out!
    val share: Byte,
)

data class Data(
    val name: String,
    /// The symbol for the asset
    val symbol: String,
    /// URI pointing to JSON representing the asset
    val uri: String,
    /// Royalty basis points that goes to creators in secondary sales (0-10000)
    val sellerFeeBasisPoints: Short,
    /// Array of creators, optional
    val creators: List<Creator>?,
)

data class CreateMetadataAccountArgs(
    val data: Data,
    val mutable: Boolean
) : MetadataInstruction()

private fun ByteBuffer.readBoolean(): Boolean {
    return get().toInt() == 1
}

private fun ByteBuffer.readString(): String {
    val length: Int = getInt()
    val bytes = ByteArray(length).apply { get(this) }

    return String(bytes, StandardCharsets.UTF_8)
}

private fun ByteBuffer.parseCreator(): Creator {
    val address = ByteArray(32).apply { get(this) }
    val verified = readBoolean()
    val share = get()

    return Creator(address, verified, share)
}

private fun ByteBuffer.parseCreateMetadataAccountArgs(): CreateMetadataAccountArgs {
    val name = readString()
    val symbol = readString()
    val uri = readString()
    val sellerFeeBasisPoints = getShort()
    val creators = if (get().toInt() == 0) null else (1..getInt()).map { parseCreator() }
    val mutable = readBoolean()

    return CreateMetadataAccountArgs(
        Data(
            name,
            symbol,
            uri,
            sellerFeeBasisPoints,
            creators
        ),
        mutable
    )
}

fun ByteArray.parseMetadataInstruction(): MetadataInstruction? {
    val buffer = ByteBuffer.wrap(this).apply { order(ByteOrder.LITTLE_ENDIAN) }

    return when (buffer.get().toInt()) {
        0 -> buffer.parseCreateMetadataAccountArgs()
        else -> null
    }
}