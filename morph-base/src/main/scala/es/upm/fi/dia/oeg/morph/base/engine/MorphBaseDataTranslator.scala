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
     * Create a list of one RDF term (as JENA resources) according to the term type from a value.
     * Although there will be always one RDF node, the method still returns a list;
     * this is a convenience as, in xR2RML, all references are potentially multi-valued.
     *
     * @param termType term type of the current term map
     * @param dbValue value read from the db, this may be a string, integer, boolean etc.,
     * @param datatype URI of the data type
     * @param language language tag
     * @return a list of one RDF node (cannot be empty)
     */
    protected def translateSingleValue(termType: String, dbValue: Object, datatype: Option[String], language: Option[String]): List[RDFNode] = {
        translateMultipleValues(termType, List(dbValue), datatype, language)
    }

    /**
     * Create a list of RDF terms (as JENA resources) according to the term type from a list of values.
     * In case of RDF collection or container, the list returned contains one RDF node that
     * is the head of the collection or container.
     *
     * @param termType term type of the current term map
     * @param values list of values: these may be strings, integers, booleans etc.,
     * @param datatype URI of the data type
     * @param language language tag
     * @return a list of RDF nodes, possibly empty
     */
    protected def translateMultipleValues(termType: String, values: List[Object], datatype: Option[String], language: Option[String]): List[RDFNode] = {

        val result: List[RDFNode] =
            // If the term type is one of R2RML term types then create one RDF term for each of the values
            if (termType == Constants.R2RML_IRI_URI ||
                termType == Constants.R2RML_LITERAL_URI ||
                termType == Constants.R2RML_BLANKNODE_URI) {
                values.flatMap(value => {
                    if (value == null) // case when the database returned NULL
                        None
                    else {
                        val node = termType match {
                            case Constants.R2RML_IRI_URI => this.createIRI(value.toString)
                            case Constants.R2RML_LITERAL_URI => this.createLiteral(value, datatype, language)
                            case Constants.R2RML_BLANKNODE_URI => {
                                var rep = GeneralUtility.encodeReservedChars(GeneralUtility.encodeUnsafeChars(value.toString))
                                this.materializer.model.createResource(new AnonId(rep))
                            }
                        }
                        Some(node)
                    }
                })
            } else {
                // If the term type is one of xR2RML collection/container term types,
                // then create one single RDF term that gathers all the values
                /**
                 * @TODO Implementation of NestTermMaps.
                 * Here we pass the datatype and languageTag for the elements of the collection, but this is incorrect:
                 * they must be given by a nestedTermType of by inferred defaults.
                 */
                val res = createCollection(termType, values, datatype, language)
                if (res.isDefined)
                    List(res.get)
                else
                    // If the res is empty, then do not return a list with one empty list inside, but just an empty list
                    List()
            }

        logger.trace("    Translated values [" + values + "] into [" + result + "]")
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
     * Convert a list of values (string, integer, boolean, etc) into an RDF collection or container of literals.
     * If the list of values is empty, return None.
     */
    def createCollection(termType: String, values: List[Object], datatype: Option[String], languageTag: Option[String]): Option[RDFNode] = {

        if (values.isEmpty)
            return None

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
        Some(translated)
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