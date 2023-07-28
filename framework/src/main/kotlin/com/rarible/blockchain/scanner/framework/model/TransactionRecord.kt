package com.rarible.blockchain.scanner.framework.model

import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
interface TransactionRecord : Record
