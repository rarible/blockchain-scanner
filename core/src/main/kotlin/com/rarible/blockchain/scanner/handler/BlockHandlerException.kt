package com.rarible.blockchain.scanner.handler

class BlockHandlerException : RuntimeException {
    constructor(message: String, cause: Throwable): super(message, cause)
}
