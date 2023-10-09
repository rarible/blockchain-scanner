package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.service.AsyncFlowAccessApi
import com.rarible.blockchain.scanner.flow.service.AsyncFlowAccessApiImpl
import com.rarible.blockchain.scanner.flow.service.FlowApiFactory
import com.rarible.blockchain.scanner.flow.service.Spork
import io.grpc.ManagedChannel
import org.onflow.protobuf.access.AccessAPIGrpc

class FlowApiFactoryStub(val channel: ManagedChannel) : FlowApiFactory {
    override fun getApi(spork: Spork): AsyncFlowAccessApi = AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
}
