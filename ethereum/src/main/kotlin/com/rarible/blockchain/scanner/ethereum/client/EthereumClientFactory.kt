package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.ethereum.client.hyper.HyperArchiveEthereumClient
import com.rarible.blockchain.scanner.ethereum.client.hyper.HyperBlockArchiverAdapter
import com.rarible.blockchain.scanner.ethereum.client.hyper.CachedHyperBlockArchiver
import com.rarible.blockchain.scanner.ethereum.client.hyper.HyperBlockArchiver
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainClientFactory
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.stereotype.Component
import scalether.core.MonoEthereum
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

@Component
@Suppress("SpringJavaInjectionPointsAutowiringInspection")
class EthereumClientFactory(
    private val mainEthereum: MonoEthereum,
    private val reconciliationEthereum: MonoEthereum,
    private val blockRepository: BlockRepository,
    private val properties: EthereumScannerProperties,
) : BlockchainClientFactory<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor> {

    @ExperimentalCoroutinesApi
    override fun createMainClient(): EthereumClient = EthereumClient(mainEthereum, properties)

    @ExperimentalCoroutinesApi
    override fun createReconciliationClient(): EthereumClient = EthereumClient(reconciliationEthereum, properties)

    @ExperimentalCoroutinesApi
    override fun createReindexClient(): BlockchainClient<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor> {
        return if (properties.hyperArchive.enabled) createHyperArchiveEthereumClient() else createMainClient()
    }

    @ExperimentalCoroutinesApi
    private fun createHyperArchiveEthereumClient(): HyperArchiveEthereumClient {
        // Create S3AsyncClient with credentials from properties
        val s3Client = S3AsyncClient.builder()
            .region(Region.AP_NORTHEAST_1)
            .credentialsProvider(
                if (properties.hyperArchive.s3.accessKeyId.isNotBlank() &&
                    properties.hyperArchive.s3.secretAccessKey.isNotBlank()
                ) {
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                            properties.hyperArchive.s3.accessKeyId,
                            properties.hyperArchive.s3.secretAccessKey
                        )
                    )
                } else {
                    null
                }
            )
            .build()

        val hyperBlockArchiver = HyperBlockArchiver(s3Client, properties.hyperArchive)
        val cachedHyperBlockArchiver = CachedHyperBlockArchiver(hyperBlockArchiver, properties.hyperArchive)
        val hyperBlockArchiverAdapter = HyperBlockArchiverAdapter(cachedHyperBlockArchiver)
        return HyperArchiveEthereumClient(hyperBlockArchiverAdapter, blockRepository, properties)
    }
}
