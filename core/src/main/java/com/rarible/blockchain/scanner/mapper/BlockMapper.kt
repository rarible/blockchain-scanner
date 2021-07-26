package com.rarible.blockchain.scanner.mapper

import com.rarible.blockchain.scanner.model.Block
import com.rarible.blockchain.scanner.model.LogEvent

interface BlockMapper<OB, B : Block, L : LogEvent> {

}