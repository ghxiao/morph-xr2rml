package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import java.sql.ResultSet

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFList
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.vocabulary.RDF

import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.path.JSONPath_PathExpression
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath
import es.upm.fi.dia.oeg.morph.base.sql.DatatypeMapper
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLNestedTermMap
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoUtils

class MorphJsondocDataTranslator(
    md: R2RMLMappingDocument,
    materializer: MorphBaseMaterializer,
    unfolder: MorphJsondocUnfolder,
    dataSourceReader: MorphBaseDataSourceReader,
    connection: GenericConnection, properties: MorphProperties)

        extends MorphBaseDataTranslator(md, materializer, unfolder, dataSourceReader, connection, properties)
        with MorphR2RMLElementVisitor {

    override val logger = Logger.getLogger(this.getClass().getName());

    override def translateData(triplesMap: MorphBaseClassMapping): Unit = {
        val query = this.unfolder.unfoldConceptMapping(triplesMap);
        this.generateRDFTriples(triplesMap, query);
    }

    override def generateRDFTriples(cm: MorphBaseClassMapping, query: GenericQuery) = {
        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
        val logicalTable = triplesMap.logicalSource;
        val sm = triplesMap.subjectMap;
        val poms = triplesMap.predicateObjectMaps;

        this.generateRDFTriples(logicalTable, sm, poms, query);
    }

    /**
     * Query the database and build triples from the result. For each row of the result set,
     * (1) create a subject resource and an optional graph resource if the subject map contains a rr:graph/rr:graphMap property,
     * (2) loop on each predicate-object map: create a list of resources for the predicates, a list of resources for the objects,
     * a list of resources from the subject map of a parent object map in case there are referencing object maps,
     * and a list of resources representing target graphs mentioned in the predicate-object map.
     * (3) Finally combine all subject, graph, predicate and object resources to generate triples.
     */
    private def generateRDFTriples(logicalTable: xR2RMLLogicalSource, sm: R2RMLSubjectMap, poms: Iterable[R2RMLPredicateObjectMap], query: GenericQuery) = {

        logger.info("Starting translating data into RDF instances...");
        if (sm == null) {
            val errorMessage = "No SubjectMap is defined";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }

        // Execute the query against the database, choose the execution method depending on the db type
        val resultSet: Iterator[String] = this.connection.dbType match {
            case Constants.DatabaseType.MongoDB => { MongoUtils.execute(this.connection, query) }
            case _ => { throw new Exception("Unsupported query type: should be an MongoDB query") }
        }

        // Apply the iterator to the result set, this creates a new result set
        val resultSetL = resultSet.toList
        val resultSetIter =
            if (logicalTable.docIterator.isDefined) {
                val jPath = JSONPath_PathExpression.parseRaw(logicalTable.docIterator.get)
                resultSetL.flatMap(result => jPath.evaluate(result).map(value => value.toString))
            } else resultSetL
        logger.trace("Query returned " + resultSetL.size + " results, " + resultSetIter.size + " results after applying the iterator.")

        // Main loop: iterate and process each result document of the result set
        var i = 0;
        for (document <- resultSetIter) {
            i = i + 1;
            logger.debug("Generating triples for document " + i + ": " + document)

            try {
                // Create the subject resource
                val subject = this.translateData(sm, document)
                if (subject == null) { throw new Exception("null value in the subject triple") }
                logger.debug("Document " + i + " subjects: " + subject)

                // Create the list of resources representing subject target graphs
                val subjectGraphs = sm.graphMaps.map(sgmElement => {
                    val subjectGraphValue = this.translateData(sgmElement, document)
                    val graphMapTermType = sgmElement.inferTermType;
                    val subjectGraph = graphMapTermType match {
                        case Constants.R2RML_IRI_URI => {
                            subjectGraphValue
                        }
                        case _ => {
                            val errorMessage = "GraphMap's TermType is not valid: " + graphMapTermType;
                            logger.warn(errorMessage);
                            throw new Exception(errorMessage);
                        }
                    }
                    subjectGraph
                });
                if (!subjectGraphs.isEmpty)
                    logger.trace("Document " + i + " subject graphs: " + subjectGraphs)

                // Add subject resource to the JENA model with its class (rdf:type) and target graphs
                sm.classURIs.foreach(classURI => {
                    val classRes = this.materializer.model.createResource(classURI);
                    if (subjectGraphs == null || subjectGraphs.isEmpty) {
                        for (sub <- subject) {
                            this.materializer.materializeQuad(sub, RDF.`type`, classRes, null);
                            this.materializer.outputStream.flush();
                        }
                    } else {
                        subjectGraphs.foreach(subjectGraph => {
                            for (sub <- subject)
                                for (subG <- subjectGraph)
                                    this.materializer.materializeQuad(sub, RDF.`type`, classRes, subG);
                        });
                    }
                });

                // Internal loop on each predicate-object map
                poms.foreach(pom => {

                    // Make a list of resources for the predicate maps of this predicate-object map
                    val predicates = pom.predicateMaps.map(predicateMap => {
                        this.translateData(predicateMap, document)
                    });
                    logger.debug("Document " + i + " predicates: " + predicates)

                    // Make a list of resources for the object maps of this predicate-object map
                    val objects = pom.objectMaps.map(objectMap => {
                        this.translateData(objectMap, document)
                    });
                    logger.debug("Document " + i + " objects: " + objects)

                    /* ####################################################################################
                     * Need to update treatment of ReferencingObjectMaps in xR2RML context
                     * ####################################################################################
                     */
                    // In case of a ReferencingObjectMaps, get the object IRI from the subject map of the parent triples map  
                    val refObjects = pom.refObjectMaps.map(refObjectMap => {
                        val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap)
                        val parentSubjectMap = parentTriplesMap.subjectMap;
                        val parentSubjects = this.translateData(parentSubjectMap, document)
                        parentSubjects
                    })
                    if (!refObjects.isEmpty)
                        logger.trace("Document " + i + " refObjects: " + refObjects)

                    // Create the list of resources representing target graphs mentioned in the predicate-object map
                    val pogm = pom.graphMaps;
                    val predicateObjectGraphs = pogm.map(pogmElement => {
                        val poGraphValue = this.translateData(pogmElement, document)
                        poGraphValue
                    });
                    if (!predicateObjectGraphs.isEmpty)
                        logger.trace("Document" + i + " predicate-object map graphs: " + predicateObjectGraphs)

                    // Finally, combine all the terms to generate triples in the target graphs or default graph
                    if (sm.graphMaps.isEmpty && pogm.isEmpty) {
                        predicates.foreach(predicatesElement => {
                            objects.foreach(objectsElement => {
                                for (sub <- subject) {
                                    for (predEl <- predicatesElement) {
                                        for (obj <- objectsElement) {
                                            this.materializer.materializeQuad(sub, predEl, obj, null)
                                            logger.debug("Materialized triple: [" + sub + "] [" + predEl + "] [" + obj + "]")
                                        }
                                    }
                                }
                            });

                            refObjects.foreach(refObjectsElement => {
                                for (sub <- subject) {
                                    for (predEl <- predicatesElement) {
                                        for (obj <- refObjectsElement) {
                                            if (obj != null) {
                                                this.materializer.materializeQuad(sub, predEl, obj, null)
                                                logger.debug("Materialized triple: [" + sub + "] [" + predEl + "] [" + obj + "]")
                                            }
                                        }
                                    }
                                }
                            });
                        });
                    } else {
                        val unionGraphs = subjectGraphs ++ predicateObjectGraphs
                        unionGraphs.foreach(unionGraph => {
                            predicates.foreach(predicatesElement => {
                                objects.foreach(objectsElement => {
                                    unionGraphs.foreach(unionGraph => {
                                        for (sub <- subject) {
                                            for (predEl <- predicatesElement) {
                                                for (obj <- objectsElement) {
                                                    for (un <- unionGraph) {
                                                        this.materializer.materializeQuad(sub, predEl, obj, un)
                                                        logger.debug("Materialized triple: graph[" + un + "], [" + sub + "] [" + predEl + "] [" + obj + "]")
                                                    }
                                                }
                                            }
                                        }
                                    });
                                });

                                refObjects.foreach(refObjectsElement => {
                                    for (sub <- subject) {
                                        for (predEl <- predicatesElement) {
                                            for (obj <- refObjectsElement) {
                                                for (un <- unionGraph) {
                                                    if (obj != null) {
                                                        this.materializer.materializeQuad(sub, predEl, obj, un)
                                                        logger.debug("Materialized triple: graph[" + un + "], [" + sub + "] [" + predEl + "] [" + obj + "]")
                                                    }
                                                }
                                            }
                                        }
                                    }
                                });
                            });
                        })
                    }

                });

            } catch {
                case e: Exception => {
                    logger.error("error while translating data: " + e.getMessage());
                    throw e
                }
            }
        }

        logger.info(i + " instances retrieved.");
    }

    /**
     * Create a list of one RDF term (as JENA resource) from one value read from the database,
     * according to the term type specified in the term map.
     * Although there will be always one RDF node, the method still returns a list;
     * this is a convenience as, in xR2RML, all references are potentially multi-valued.
     *
     * @param termMap current term map
     * @param dbValue value to translate, this may be a string, integer, boolean etc.
     * @param datatype URI of the data type
     * @return a list of RDF nodes
     */
    private def translateSingleValue(termMap: R2RMLTermMap, dbValue: Object, datatype: Option[String]): List[RDFNode] = {
        translateMultipleValues(termMap, List(dbValue), datatype)
    }

    /**
     * Create a list of RDF terms (as JENA resources) from a list of values
     * according to the term type specified in the term map.
     * In case of RDF collection or container, the list returned contains one RDF node that
     * is the head of the collection or container.
     *
     * @param termMap current term map
     * @param values list of values: these may be strings, integers, booleans etc.,
     * @param datatype URI of the data type
     * @return a list of RDF nodes
     */
    private def translateMultipleValues(termMap: R2RMLTermMap, values: List[Object], datatype: Option[String]): List[RDFNode] = {

        val result: List[RDFNode] =
            // If the term type is one of R2RML term types then create one RDF term for each of the values
            if (termMap.inferTermType == Constants.R2RML_IRI_URI ||
                termMap.inferTermType == Constants.R2RML_LITERAL_URI ||
                termMap.inferTermType == Constants.R2RML_BLANKNODE_URI) {
                values.map(value => {
                    if (value == null) // case when the database returned NULL
                        this.createLiteral("", datatype, termMap.languageTag)
                    else {
                        termMap.inferTermType match {
                            case Constants.R2RML_IRI_URI => this.createIRI(value.toString)
                            case Constants.R2RML_LITERAL_URI => this.createLiteral(value, datatype, termMap.languageTag)
                            case Constants.R2RML_BLANKNODE_URI => {
                                var rep = GeneralUtility.encodeReservedChars(GeneralUtility.encodeUnsafeChars(value.toString))
                                this.materializer.model.createResource(new AnonId(rep))
                            }
                        }
                    }
                })
            } else {

                // If the term type is one of xR2RML collection/container term types,
                // then create one single RDF term that gathers all the values
                val translated: RDFNode = termMap.inferTermType match {
                    case xR2RML_Constants.xR2RML_RDFLIST_URI => {
                        val valuesAsRdfNodes = values.map(value => this.createLiteral(value, datatype, termMap.languageTag))
                        val node = this.materializer.model.createList(valuesAsRdfNodes.iterator)
                        node
                    }
                    case xR2RML_Constants.xR2RML_RDFBAG_URI => {
                        var list = this.materializer.model.createBag()
                        for (value <- values)
                            list.add(this.createLiteral(value, datatype, termMap.languageTag))
                        list
                    }
                    case xR2RML_Constants.xR2RML_RDFALT_URI => {
                        val list = this.materializer.model.createAlt()
                        for (value <- values)
                            list.add(this.createLiteral(value, datatype, termMap.languageTag))
                        list
                    }
                    case xR2RML_Constants.xR2RML_RDFSEQ_URI => {
                        val list = this.materializer.model.createSeq()
                        for (value <- values)
                            list.add(this.createLiteral(value, datatype, termMap.languageTag))
                        list
                    }
                    case _ => { throw new Exception("Unkown term type: " + termMap.inferTermType) }
                }
                List(translated)
            }

        logger.trace("    Translated values [" + values + "] into [" + result + "]")
        result
    }

    /**
     * Apply a term map to the current document of the result set, and generate a list of RDF terms:
     * for each element reference in the term map (reference or template), read values from the current document,
     * translate them into RDF terms.
     */
    private def translateData(termMap: R2RMLTermMap, jsonDoc: String): List[RDFNode] = {

        var result: List[RDFNode] = List(null);

        val dbType = this.properties.databaseType;
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val inferedTermType = termMap.inferTermType();

        result = termMap.termMapType match {

            // --- Constant-valued term map
            case Constants.MorphTermMapType.ConstantTermMap => {
                this.translateSingleValue(termMap, termMap.constantValue, termMap.datatype)
            }

            // --- Reference-valued term map
            case Constants.MorphTermMapType.ReferenceTermMap => {

                // Evaluate the value against the mixed syntax path
                val msPath = termMap.getMixedSyntaxPaths()(0)
                val values: List[Object] = msPath.evaluate(jsonDoc)

                // Generate RDF terms from the values resulting from the evaluation
                this.translateMultipleValues(termMap, values, termMap.datatype);
            }

            // --- Template-valued term map
            case Constants.MorphTermMapType.TemplateTermMap => {

                // For each group of the template, compute a list of replacement strings
                val msPaths = termMap.getMixedSyntaxPaths()
                val listReplace = for (i <- 0 to (msPaths.length - 1)) yield {

                    // Evaluate the raw value against the mixed-syntax path.
                    val valuesRaw: List[Object] = msPaths(i).evaluate(jsonDoc)
                    //valuesRaw.filter(_ != null)
                    valuesRaw
                }

                val replacements: List[List[Object]] = listReplace.toList
                logger.trace("Template replacements: " + replacements)

                // Replace "{...}" groups in the template string with corresponding values from the db
                if (replacements.isEmpty) {
                    logger.warn("Template " + termMap.templateString + ": no group to replace with values from the DB.")
                    null
                } else {
                    // Compute the list of template results by making all possible combinations of the replacement values
                    val templates = TemplateUtility.replaceTemplateGroups(termMap.templateString, replacements);
                    this.translateMultipleValues(termMap, templates, termMap.datatype);
                }
            }

            case _ => { throw new Exception("Invalid term map type " + termMap.termMapType) }
        }
        result
    }

    private def getResultSetValue(termMap: R2RMLTermMap, rs: ResultSet, pColumnName: String): Object = {
        try {
            val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME);
            val tableName = zConstant.table;
            val columnName = {
                if (tableName != null) {
                    tableName + "." + zConstant.column
                } else
                    zConstant.column
            }

            val result = if (termMap.datatype == null) {
                rs.getString(columnName);
            } else if (termMap.datatype.equals(XSDDatatype.XSDdateTime.getURI())) {
                rs.getDate(columnName).toString();
            } else {
                rs.getObject(columnName);
            }
            result
        } catch {
            case e: Exception => {
                e.printStackTrace();
                logger.error("An error occured when reading the SQL result set : " + e.getMessage());
                null
            }
        }
    }

    /**
     * Create a JENA literal resource with optional data type and language tag.
     * This method is overriden in the case of JSON to enable the mapping between JSON data types
     * and XSD data types
     */
    override protected def createLiteral(value: Object, datatype: Option[String], language: Option[String]): Literal = {
        try {
            val encodedValue =
                if (value == null) // case when the database returned NULL
                    ""
                else
                    GeneralUtility.encodeLiteral(value.toString())

            val dataT: String = datatype.getOrElse(null)
            val valueConverted =
                if (dataT != null) {
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
                    else {
                        val inferedDT = inferDataType(value)
                        if (inferedDT == null)
                            this.materializer.model.createLiteral(valueConverted);
                        else
                            this.materializer.model.createTypedLiteral(valueConverted, inferedDT);
                    }
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
     * Defines the mapping from JSON data types to XSD data types
     */
    private def inferDataType(value: Object): String = {
        value match {
            case _: java.lang.Byte => XSDDatatype.XSDinteger.getURI()
            case _: java.lang.Short => XSDDatatype.XSDinteger.getURI()
            case _: java.lang.Integer => XSDDatatype.XSDinteger.getURI()
            case _: java.lang.Long => XSDDatatype.XSDinteger.getURI()

            case _: java.lang.Double => XSDDatatype.XSDdecimal.getURI()
            case _: java.lang.Float => XSDDatatype.XSDdecimal.getURI()

            case _: java.lang.Boolean => XSDDatatype.XSDboolean.getURI()

            case _: java.lang.Number => XSDDatatype.XSDdecimal.getURI()

            case _ => null
        }

    }

    def visit(logicalTable: xR2RMLLogicalSource): Object = {
        throw new Exception("Unsopported method.")
    }

    override def visit(mappingDocument: R2RMLMappingDocument): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(objectMap: R2RMLObjectMap): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(refObjectMap: R2RMLRefObjectMap): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(r2rmlTermMap: R2RMLTermMap): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(triplesMap: R2RMLTriplesMap): Object = {
        throw new Exception("Unsopported method.")
    }
}