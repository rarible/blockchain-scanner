package com.rarible.blockchain.scanner.flow.mongo

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.nftco.flow.sdk.cadence.AddressField
import com.nftco.flow.sdk.cadence.ArrayField
import com.nftco.flow.sdk.cadence.BooleanField
import com.nftco.flow.sdk.cadence.CapabilityField
import com.nftco.flow.sdk.cadence.CapabilityValue
import com.nftco.flow.sdk.cadence.CompositeAttribute
import com.nftco.flow.sdk.cadence.CompositeField
import com.nftco.flow.sdk.cadence.CompositeValue
import com.nftco.flow.sdk.cadence.DictionaryField
import com.nftco.flow.sdk.cadence.DictionaryFieldEntry
import com.nftco.flow.sdk.cadence.Field
import com.nftco.flow.sdk.cadence.NumberField
import com.nftco.flow.sdk.cadence.OptionalField
import com.nftco.flow.sdk.cadence.PathField
import com.nftco.flow.sdk.cadence.PathValue
import com.nftco.flow.sdk.cadence.StaticType
import com.nftco.flow.sdk.cadence.StringField
import com.nftco.flow.sdk.cadence.TypeField
import com.nftco.flow.sdk.cadence.TypeValue
import com.nftco.flow.sdk.cadence.VoidField
import com.rarible.core.mongo.converter.CustomMongoConverter
import org.bson.Document
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component
import java.util.Optional
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

@Component
class FlowFieldCustomMongoConverter : CustomMongoConverter {

    override fun getConverters(): MutableList<Converter<*, *>> {
        return mutableListOf(FlowFieldConverter)
    }

    override fun isSimpleType(aClass: Class<*>?): Boolean {
        return aClass != null && aClass.isAssignableFrom(Field::class.java)
    }

    override fun getCustomWriteTarget(sourceType: Class<*>?): Optional<Class<*>> {
        return Optional.empty()
    }
}

object FlowFieldConverter : Converter<Document, Field<*>> {

    private val byType = Field::class.java.getAnnotation(JsonSubTypes::class.java).value.associateBy(
        { it.name }, { it.value }
    )

    private val mapping = mapOf<KClass<*>, (source: Document) -> Any>(
        StringField::class to { StringField(toStr(it)) },
        AddressField::class to { AddressField(toStr(it)) },
        BooleanField::class to { BooleanField(toBoolean(it)) },
        VoidField::class to { VoidField() },

        OptionalField::class to { OptionalField(toOptional(it)) },
        ArrayField::class to { ArrayField(toArray(it)) },
        DictionaryField::class to { DictionaryField(toDictionary(it)) },
        PathField::class to { PathField(toPath(it)) },
        CapabilityField::class to { CapabilityField(toCapability(it)) },
        TypeField::class to { TypeField(toType(it)) }
    )

    override fun convert(source: Document): Field<*>? {
        val type = source.getString("type")
        val klass = byType[type]!!

        // Direct mapping
        mapping[klass]?.let { return it(source) as Field<*> }

        // Subclasses mapping
        return when {
            klass.isSubclassOf(NumberField::class) -> {
                // All Fields has only one constructor with single argument
                val constructor = klass.constructors.find { true }!!
                constructor.call(toStr(source))
            }

            klass.isSubclassOf(CompositeField::class) -> {
                val constructor = klass.constructors.find { true }!!
                constructor.call(toComposite(source))
            }

            else -> throw UnsupportedOperationException("Unable to parse Flow field (unknown type): $source")
        } as Field<*>?
    }

    private fun toStr(source: Document): String {
        return source.getString("value")
    }

    private fun toBoolean(source: Document): Boolean {
        return source.getBoolean("value")
    }

    private fun toArray(source: Document): Array<Field<*>> {
        val list = (source["value"] as List<*>?) ?: emptyList<Any>()
        return list.mapNotNull { convert(it as Document) }.toTypedArray()
    }

    private fun toOptional(source: Document): Field<*>? {
        val field = source["value"] ?: return null
        return convert(field as Document)
    }

    private fun toDictionary(source: Document): Array<DictionaryFieldEntry> {
        val list = (source["value"] as List<*>?) ?: emptyList<Any>()
        return list.map {
            val entry = it as Document
            val key = convert(entry["key"] as Document)!!
            val value = convert(entry["value"] as Document)!!
            DictionaryFieldEntry(key, value)
        }.toTypedArray()
    }

    private fun toPath(source: Document): PathValue {
        val document = source["value"] as Document
        return PathValue(
            document.getString("domain"),
            document.getString("identifier")
        )
    }

    private fun toCapability(source: Document): CapabilityValue {
        val document = source["value"] as Document
        return CapabilityValue(
            document.getString("path"),
            document.getString("address"),
            document.getString("borrowType"),
        )
    }

    private fun toComposite(source: Document): CompositeValue {
        val document = source["value"] as Document
        val id = document.getString("id")
        val fields = (source["fields"] as List<*>?) ?: emptyList<Any>()
        val attributes = fields.map {
            val attr = it as Document
            CompositeAttribute(
                attr.getString("name"),
                convert(attr["value"] as Document)!!
            )
        }.toTypedArray()

        return CompositeValue(id, attributes)
    }

    private fun toType(source: Document): TypeValue {
        val document = source["value"] as Document
        val static = document["staticType"] as Document
        return TypeValue(
            staticType = StaticType(
                kind = static.getString("kind"),
                typeID = static.getString("typeID")
            )
        )
    }
}
