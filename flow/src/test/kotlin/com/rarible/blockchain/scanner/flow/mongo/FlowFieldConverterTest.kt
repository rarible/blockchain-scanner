package com.rarible.blockchain.scanner.flow.mongo

import com.nftco.flow.sdk.cadence.AddressField
import com.nftco.flow.sdk.cadence.ArrayField
import com.nftco.flow.sdk.cadence.BooleanField
import com.nftco.flow.sdk.cadence.CapabilityField
import com.nftco.flow.sdk.cadence.CapabilityValue
import com.nftco.flow.sdk.cadence.DictionaryField
import com.nftco.flow.sdk.cadence.DictionaryFieldEntry
import com.nftco.flow.sdk.cadence.Field
import com.nftco.flow.sdk.cadence.Fix64NumberField
import com.nftco.flow.sdk.cadence.Int256NumberField
import com.nftco.flow.sdk.cadence.IntNumberField
import com.nftco.flow.sdk.cadence.OptionalField
import com.nftco.flow.sdk.cadence.PathField
import com.nftco.flow.sdk.cadence.PathValue
import com.nftco.flow.sdk.cadence.StaticType
import com.nftco.flow.sdk.cadence.StringField
import com.nftco.flow.sdk.cadence.TypeField
import com.nftco.flow.sdk.cadence.TypeValue
import com.nftco.flow.sdk.cadence.VoidField
import com.rarible.blockchain.scanner.flow.test.IntegrationTest
import com.rarible.core.test.data.randomString
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.mapping.Document

@IntegrationTest
class FlowFieldConverterTest {

    @Autowired
    lateinit var mongo: ReactiveMongoTemplate

    @Test
    fun `string - ok`() = runBlocking {
        saveAndCheck(StringField("abc"))
        saveAndCheck(AddressField("0xf8d6e0586b0a20c7"))
        saveAndCheck(IntNumberField("54"))
    }

    @Test
    fun `boolean - ok`() = runBlocking {
        saveAndCheck(BooleanField(true))
    }

    @Test
    fun `void - ok`() = runBlocking {
        saveAndCheck(VoidField())
    }

    @Test
    fun `optional - ok`() = runBlocking {
        saveAndCheck(OptionalField(IntNumberField("678")))
    }

    @Test
    fun `array - ok`() = runBlocking {
        saveAndCheck(
            ArrayField(
                listOf(
                    VoidField(),
                    BooleanField(true)
                ).toTypedArray()
            )
        )
    }

    @Test
    fun `path - ok`() = runBlocking {
        saveAndCheck(PathField(PathValue(randomString(), randomString())))
    }

    @Test
    fun `type - ok`() = runBlocking {
        saveAndCheck(TypeField(TypeValue(StaticType(randomString(), randomString()))))
    }

    @Test
    fun `capability - ok`() = runBlocking {
        saveAndCheck(CapabilityField(CapabilityValue(randomString(), randomString(), randomString())))
    }

    @Test
    fun `dictionary - ok`() = runBlocking {
        saveAndCheck(
            DictionaryField(
                listOf(
                    DictionaryFieldEntry(StringField("key"), Fix64NumberField("64")),
                    DictionaryFieldEntry(BooleanField(true), Int256NumberField("123")),
                )
            )
        )
    }

    private suspend fun saveAndCheck(field: Field<*>) {
        val id = randomString()
        val test = FlowField(id, field)

        mongo.save(test).awaitFirstOrNull()

        val fromDb = mongo.findById(id, FlowField::class.java).awaitFirst()

        assertThat(fromDb.field).isEqualTo(field)
    }

}

@Document("flow_field")
data class FlowField(
    @Id
    val id: String,
    val field: Field<*>
)