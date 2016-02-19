package es.upm.fi.dia.oeg.morph.base.engine

import scala.collection.JavaConversions.asJavaIterator

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.vocabulary.RDF

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource

abstract class MorphBaseDataTranslator(
        val md: R2RMLMappingDocument,
        val materializer: MorphBaseMaterializer,
        val unfolder: MorphBaseUnfolder,
        properties: MorphProperties) {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Entry point of the materialization approach:
     * Loop on all triples maps of the mapping graph and generate triples in the data materializer model,
     * based on the triples map: this consists in calculating the query, running it against the database,
     * translating results in RDF terms and making the triples.
     */
    def translateData(mappingDocument: R2RMLMappingDocument): Unit = {
        val tms = mappingDocument.classMappings
        for (tm <- tms) {
            logger.info("===============================================================================");
            logger.info("Starting data materialization of triples map " + tm.id);
            try {
                val query = this.unfolder.unfoldConceptMapping(tm);
                this.generateRDFTriples(tm, query);
                null
            } catch {
                case e: MorphException => {
                    logger.error("Error while generatring triples for " + tm + ": " + e.getMessage);
                    e.printStackTrace()
                }
                case e: Exception => {
                    logger.error("Unexpected error while generatring triples for " + tm + ": " + e.getMessage);
                    e.printStackTrace()
                }
            }
        }
    }

    /**
     * Generate triples in the context of the query rewriting: run the query and apply
     * the triples map bound to the query (GenericQuery.bondTriplesMap) to create triples.
     */
    def generateRDFTriples(query: GenericQuery): Unit = {
        val tm = query.boundTriplesMap.get
        try {
            this.generateRDFTriples(tm, query);
        } catch {
            case e: MorphException => {
                logger.error("Error while generatring triples for " + tm + ": " + e.getMessage);
                e.printStackTrace()
            }
            case e: Exception => {
                logger.error("Unexpected error while generatring triples for " + tm + ": " + e.getMessage);
                e.printStackTrace()
            }
        }
    }

    /**
     * Query the database and build triples from the result based on the triples map definition.
     * This is the method where all the database-specific work will be done
     *
     * @throws MorphException
     */
    protected def generateRDFTriples(
        tm: R2RMLTriplesMap,
        query: GenericQuery): Unit

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
     * The method always returns a List, either empty or with one RDF term in the case collecTermType is not null,
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

        if (logger.isTraceEnabled()) logger.trace("    Translated values [" + values + "] into [" + result + "]")
        result
    }

    /**
     * Create a list of RDF terms (as JENA resources) from a list of values
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
        //if (logger.isTraceEnabled()) logger.trace("    Translated values [" + values + "] into [" + result + "]")
        result
    }

    /**
     *  Create a JENA resource with an IRI after URL-encoding the string
     */
    @throws[MorphException]
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
                val msg = "Error translating object uri value : " + resultIRI
                logger.error(msg);
                throw new MorphException(msg, e)
            }
        }
    }

    /**
     * Create a JENA literal resource with optional datatype and language tag
     */
    @throws[MorphException]
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
                val msg = "Error translating object uri value : " + value
                logger.error(msg);
                throw new MorphException(msg, e)
            }
        }
    }

    /**
     * Convert a list of RDFNodes into an RDF collection or container. The result is returned as a list with one element.
     *
     * If the list of nodes is empty, return an empty list, but no empty collection/contianer is returned.
     */
    @throws[MorphException]
    protected def createCollection(collecTermType: String, values: List[RDFNode]): List[RDFNode] = {

        // If values is empty, then do not return a list with one empty list inside, but just an empty list
        if (values.isEmpty)
            return List()

        val translated: RDFNode = collecTermType match {
            case Constants.xR2RML_RDFLIST_URI => {
                val node = this.materializer.model.createList(values.iterator)
                this.materializer.model.add(node, RDF.`type`, RDF.List)
                node
            }
            case Constants.xR2RML_RDFBAG_URI => {
                var list = this.materializer.model.createBag()
                for (value <- values) list.add(value)
                list
            }
            case Constants.xR2RML_RDFALT_URI => {
                val list = this.materializer.model.createAlt()
                for (value <- values) list.add(value)
                list
            }
            case Constants.xR2RML_RDFSEQ_URI => {
                val list = this.materializer.model.createSeq()
                for (value <- values) list.add(value)
                list
            }
            case _ => {
                val msg = "Term type " + collecTermType + " is not an RDF collection/container term type"
                logger.error(msg);
                throw new MorphException(msg)
            }
        }
        List(translated)
    }

    protected def translateDateTime(value: String): String = {
        value.toString().trim().replaceAll(" ", "T");
    }

    protected def translateBoolean(value: String): String = {
        if (value.equalsIgnoreCase("T") || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1"))
            "true"
        else
            "false"
    }
}