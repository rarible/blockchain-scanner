package com.rarible.blockchain.scanner.framework.model

import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * Custom data, converted from Original Blockchain event log by target service
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
interface EventData