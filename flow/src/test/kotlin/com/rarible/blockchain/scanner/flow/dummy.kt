package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.crypto.Crypto
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import java.util.Locale

fun main() {
    val pair = Crypto.generateKeyPair()
    println(RandomStringUtils.random(16, "0123456789ABCDEF").lowercase(Locale.ENGLISH))
    println(pair.private.hex)
    println(pair.public.hex)
}
