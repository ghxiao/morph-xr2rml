package es.upm.fi.dia.oeg.morph.base.engine

import scala.collection.JavaConversions.asJavaIterator

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFNode

import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

abstract class MorphBaseDataTranslator(
        val md: MorphBaseMappingDocument,
        val materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        val dataSourceReader: MorphBaseDataSourceReader,

        /** The connection object can be anything: java.sql.Connection for an RDB, MongoDB context etc. */
        connection: GenericConnection,

        properties: MorphProperties) {

    val logger = Logger.getLogger(this.getClass().getName());

    def translateData(triplesMap: MorphBaseClassMapping);

    /**
     * @param query may be either an iQuery in the RDB case, or a simple string in case of non row-based nor SQL based databases
     */
    def generateRDFTriples(cm: MorphBaseClassMapping, query: GenericQuery);

    /**
     *  Create a JENA resource with an IRI after URL-encoding the string
     */
    protected def createIRI(originalIRI: String) = {
        var resultIRI = originalIRI;
        try {
            resultIRI = GeneralUtility.encodeURI(resultIRI, properties.mapURIEncodingChars, properties.uriTransformationOperation);
            if (this.properties.encodeUnsafeChars)
                resultIRI = GeneralUtility.encodeUnsafeChars(resultIRI);

            if (this.properties.encodeReservedChars)
                resultIRI = GeneralUtility.encodeReservedChars(resultIRI);

            this.materializer.model.createResource(resultIRI);
        } catch {
            case e: Exception => {
                logger.warn("Error translating object uri value : " + resultIRI);
                throw e
            }
        }
    }

    /**
     * Create a JENA literal resource with optional datatype and language tag
     */
    protected def createLiteral(value: Object, datatype: Option[String], language: Option[String]): Literal = {
        try {
            val encodedValue =
                if (value == null) // case when the database returned NULL
                    ""
                else
                    GeneralUtility.encodeLiteral(value.toString())

            val dataT: String = datatype.getOrElse(null)
            val valueConverted =
                if (dataT != null) { // the datatype may be Some(null), so keep this test here
                    if (dataT.equals(XSDDatatype.XSDdateTime.getURI().toString()))
                        this.translateDateTime(encodedValue);
                    else if (dataT.equals(XSDDatatype.XSDboolean.getURI().toString()))
                        this.translateBoolean(encodedValue);
                    else
                        encodedValue
                } else
                    encodedValue

            val result: Literal =
                if (language.isDefined)
                    this.materializer.model.createLiteral(valueConverted, language.get);
                else {
                    if (datatype.isDefined)
                        this.materializer.model.createTypedLiteral(valueConverted, datatype.get);
                    else
                        this.materializer.model.createLiteral(valueConverted);
                }

            result
        } catch {
            case e: Exception => {
                logger.warn("Error translating object uri value : " + value);
                throw e
            }
        }
    }

    /**
     * Convert a list of values (string, integer, boolean, etc) into an RDF collection or container of literals
     */
    def createCollection(termType: String, values: List[Object], datatype: Option[String], languageTag: Option[String]): RDFNode = {

        val translated: RDFNode = termType match {
            case xR2RML_Constants.xR2RML_RDFLIST_URI => {
                val valuesAsRdfNodes = values.map(value => this.createLiteral(value, datatype, languageTag))
                val node = this.materializer.model.createList(valuesAsRdfNodes.iterator)
                node
            }
            case xR2RML_Constants.xR2RML_RDFBAG_URI => {
                var list = this.materializer.model.createBag()
                for (value <- values)
                    list.add(this.createLiteral(value, datatype, languageTag))
                list
            }
            case xR2RML_Constants.xR2RML_RDFALT_URI => {
                val list = this.materializer.model.createAlt()
                for (value <- values)
                    list.add(this.createLiteral(value, datatype, languageTag))
                list
            }
            case xR2RML_Constants.xR2RML_RDFSEQ_URI => {
                val list = this.materializer.model.createSeq()
                for (value <- values)
                    list.add(this.createLiteral(value, datatype, languageTag))
                list
            }
            case _ => { throw new Exception("Unkown term type: " + termType) }
        }
        translated
    }

    /**
     * Convert a list of RDFNodes into an RDF collection or container
     */
    def createCollection(termType: String, values: List[RDFNode]): RDFNode = {

        val translated: RDFNode = termType match {
            case xR2RML_Constants.xR2RML_RDFLIST_URI => {
                this.materializer.model.createList(values.iterator)
            }
            case xR2RML_Constants.xR2RML_RDFBAG_URI => {
                var list = this.materializer.model.createBag()
                for (value <- values)
                    list.add(value)
                list
            }
            case xR2RML_Constants.xR2RML_RDFALT_URI => {
                val list = this.materializer.model.createAlt()
                for (value <- values)
                    list.add(value)
                list
            }
            case xR2RML_Constants.xR2RML_RDFSEQ_URI => {
                val list = this.materializer.model.createSeq()
                for (value <- values)
                    list.add(value)
                list
            }
            case _ => { throw new Exception("Unkown term type: " + termType) }
        }
        translated
    }

    protected def translateDateTime(value: String): String = {
        value.toString().trim().replaceAll(" ", "T");
    }

    protected def translateBoolean(value: String): String = {
        if (value.equalsIgnoreCase("T") || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1"))
            "true"
        else if (value.equalsIgnoreCase("F") || value.equalsIgnoreCase("False") || value.equalsIgnoreCase("0"))
            "false"
        else
            "false"
    }
}