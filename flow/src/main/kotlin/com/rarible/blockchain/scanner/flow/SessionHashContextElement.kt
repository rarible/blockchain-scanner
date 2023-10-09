package com.rarible.blockchain.scanner.flow

import java.util.UUID
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

class SessionHashContextElement : AbstractCoroutineContextElement(SessionHashContextElement) {
    val sessionHash: String = UUID.randomUUID().toString()

    companion object Key : CoroutineContext.Key<SessionHashContextElement>
}
