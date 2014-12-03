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
import com.hp.hpl.jena.rdf.model.AnonId
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.vocabulary.RDF

abstract class MorphBaseDataTranslator(
        val md: MorphBaseMappingDocument,
        val materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        val dataSourceReader: MorphBaseDataSourceReader,

        /** The connection object can be anything: java.sql.Connection for an RDB, MongoDB context etc. */
        connection: GenericConnection,

        properties: MorphProperties) {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Generate triples in the current model of the data materializer, based
     * on the current triples map: this consists in calculating the query, running it
     * against the database, translating results in RDF terms and making the triples.
     */
    def generateRDFTriples(triplesMap: MorphBaseClassMapping): Unit

    /**
     * Convert a value (string, integer, boolean, etc) into an RDF term.
     * If collecTermType is not null, the RDF term is enclosed in a term of that type, i.e. a collection or container.
     *
     * The method always returns a List, either empty or with one RDf term.
     *
     * @param dbValue value read from the db, this may be a string, integer, boolean etc.,
     * @param collecTermType term type of the RDF collection/container. May be null.
     * @param memberTermType the term type of RDF terms representing the value
     * @param datatype URI of the data type
     * @param language language tag
     * @return a list of one RDF node (cannot be empty)
     */
    protected def translateSingleValue(dbValue: Object, collecTermType: Option[String], memberTermType: String, datatype: Option[String], language: Option[String]): List[RDFNode] = {
        translateMultipleValues(List(dbValue), collecTermType, memberTermType, datatype, language)
    }

    /**
     * Convert a list of values (string, integer, boolean, etc) into RDF terms.
     * If collecTermType is not null, RDF terms are enclosed in a term of that type, i.e. a collection or container
     *
     * The method always returns a List, either empty or with one RDf term in the case collecTermType is not null,
     * or with several elements otherwise.
     *
     * @param values list of values these may be strings, integers, booleans etc.,
     * @param collecTermType term type of the RDF collection/container. May be null.
     * @param memberTermType the term type of RDF terms representing values
     * @param datatype URI of the data type
     * @param language language tag
     * @return a list of one RDF node, possibly empty
     */
    protected def translateMultipleValues(values: List[Object], collecTermType: Option[String], memberTermType: String, datatype: Option[String], languageTag: Option[String]): List[RDFNode] = {

        if (values.isEmpty)
            return List()

        // Convert values into RDF nodes
        val valuesAsRdfNodes = translateMultipleValues(values, memberTermType, datatype, languageTag)

        val result: List[RDFNode] =
            if (collecTermType.isDefined)
                // Create the collection/container with that list of nodes
                createCollection(collecTermType.get, valuesAsRdfNodes)
            else
                valuesAsRdfNodes

        logger.trace("    Translated values [" + values + "] into [" + result + "]")
        result
    }

    /**
     * Create a list of RDF terms (as JENA resources) according to the term type from a list of values.
     *
     * @param values list of values: these may be strings, integers, booleans etc.,
     * @param termType term type of the current term map
     * @param datatype URI of the data type
     * @param language language tag
     * @return a list of RDF nodes, possibly empty
     */
    private def translateMultipleValues(values: List[Object], termType: String, datatype: Option[String], languageTag: Option[String]): List[RDFNode] = {

        val result: List[RDFNode] =
            // Create one RDF term for each of the values: the flatMap eliminates None elements, thus the result can be empty
            values.flatMap(value => {
                if (value == null) // case when the database returned NULL
                    None
                else {
                    val node = termType match {
                        case Constants.R2RML_IRI_URI => this.createIRI(value.toString)
                        case Constants.R2RML_LITERAL_URI => this.createLiteral(value, datatype, languageTag)
                        case Constants.R2RML_BLANKNODE_URI => {
                            var rep = GeneralUtility.encodeReservedChars(GeneralUtility.encodeUnsafeChars(value.toString))
                            this.materializer.model.createResource(new AnonId(rep))
                        }
                    }
                    Some(node)
                }
            })
        //logger.trace("    Translated values [" + values + "] into [" + result + "]")
        result
    }

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
     * Convert a list of RDFNodes into an RDF collection or container. The result is returned as a list with one element.
     *
     * If the list of nodes is empty, return an empty list, but no empty collection/contianer is returned.
     */
    protected def createCollection(collecTermType: String, values: List[RDFNode]): List[RDFNode] = {

        // If values is empty, then do not return a list with one empty list inside, but just an empty list
        if (values.isEmpty)
            return List()

        val translated: RDFNode = collecTermType match {
            case xR2RML_Constants.xR2RML_RDFLIST_URI => {
                val node = this.materializer.model.createList(values.iterator)
                this.materializer.model.add(node, RDF.`type`, RDF.List)
                node
            }
            case xR2RML_Constants.xR2RML_RDFBAG_URI => {
                var list = this.materializer.model.createBag()
                for (value <- values) list.add(value)
                list
            }
            case xR2RML_Constants.xR2RML_RDFALT_URI => {
                val list = this.materializer.model.createAlt()
                for (value <- values) list.add(value)
                list
            }
            case xR2RML_Constants.xR2RML_RDFSEQ_URI => {
                val list = this.materializer.model.createSeq()
                for (value <- values) list.add(value)
                list
            }
            case _ => { throw new Exception("Term type " + collecTermType + " is not an RDF collection/container term type") }
        }
        List(translated)
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