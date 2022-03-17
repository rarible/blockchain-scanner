package com.rarible.blockchain.scanner.handler

class BlockHandlerException : RuntimeException {
    constructor(message: String, cause: Throwable): super(message, cause)
}

suspend fun <T> runRethrowingBlockHandlerException(actionName: String, block: suspend () -> T): T {
    return try {
        block()
    } catch (e: Exception) {
        if (e is BlockHandlerException) {
            throw e
        }
        throw BlockHandlerException("Failed to '$actionName'", e)
    }
}
