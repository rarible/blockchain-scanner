package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Descriptor
import java.util.*

class FlowDescriptor(
    override val id: String = UUID.randomUUID().toString()
): Descriptor
