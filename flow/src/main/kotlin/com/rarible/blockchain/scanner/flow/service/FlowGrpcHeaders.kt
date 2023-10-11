package com.rarible.blockchain.scanner.flow.service

import io.grpc.Metadata

val SESSION_HASH_HEADER = Metadata.Key.of("x-session-hash", Metadata.ASCII_STRING_MARSHALLER)
