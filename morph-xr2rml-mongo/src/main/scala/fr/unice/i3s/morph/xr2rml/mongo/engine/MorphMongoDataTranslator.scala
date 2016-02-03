package fr.unice.i3s.morph.xr2rml.mongo.engine

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.vocabulary.RDF

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.path.JSONPath_PathExpression
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import fr.unice.i3s.morph.xr2rml.mongo.MongoUtils

class MorphMongoDataTranslator(
    md: R2RMLMappingDocument,
    materializer: MorphBaseMaterializer,
    unfolder: MorphMongoUnfolder,
    connection: GenericConnection, properties: MorphProperties)

        extends MorphBaseDataTranslator(md, materializer, unfolder, connection, properties) {

    if (!connection.isMongoDB)
        throw new MorphException("Database connection type does not match MongoDB")

    /** Store already executed queries to avoid running them several times. The key of the hashmap is the query string itself. */
    private var executedQueries: scala.collection.mutable.Map[String, List[String]] = new scala.collection.mutable.HashMap

    /** Store already parsed queries to avoid creating the same GenericQuery object several times. The key of the hashmap is the triples map name */
    private var queries: scala.collection.mutable.Map[String, GenericQuery] = new scala.collection.mutable.HashMap

    override val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Query the database and build triples from the result. For each document of the result set:
     *
     * <ol>
     * <li>create a subject resource and an optional graph resource if the subject map contains a rr:graph/rr:graphMap property,</li>
     * <li>loop on each predicate-object map: create a list of resources for the predicates, a list of resources for the objects,
     * a list of resources from the subject map of a parent object map in case there are referencing object maps,
     * and a list of resources representing target graphs mentioned in the predicate-object map.</li>
     * <li>Finally combine all subject, graph, predicate and object resources to generate triples.</li>
     * </ol>
     */
    @throws[MorphException]
    override def generateRDFTriples(logicalSrc: Option[xR2RMLLogicalSource], sm: R2RMLSubjectMap, poms: Iterable[R2RMLPredicateObjectMap], query: GenericQuery): Unit = {

        logger.info("Starting translating triples map into RDF instances...");
        if (sm == null) {
            val errorMessage = "No SubjectMap is defined";
            logger.error(errorMessage);
            throw new MorphException(errorMessage);
        }

        // Execute the query against the database and apply the iterator
        val resultSet =
            if (logicalSrc.isDefined)
                executeQueryAndIteraotr(query, logicalSrc.get.docIterator)
            else
                executeQueryAndIteraotr(query, None)

        // Main loop: iterate and process each result document of the result set
        var i = 0;
        for (document <- resultSet) {
            i = i + 1;
            if (logger.isDebugEnabled()) logger.debug("Generating triples for document " + i + "/" + resultSet.size + ": " + document)
            if (logger.isInfoEnabled()) System.out.print("Generating triples for document: " + i + "/" + resultSet.size + "          \r")

            try {
                // Create the subject resource
                val subjects = this.translateData(sm, document)
                if (subjects == null) { throw new Exception("null value in the subject triple") }
                if (logger.isDebugEnabled()) logger.debug("Document " + i + " subjects: " + subjects)

                // Create the list of resources representing subject target graphs
                val subjectGraphs = sm.graphMaps.map(sgmElement => {
                    val subjectGraphValue = this.translateData(sgmElement, document)
                    val graphMapTermType = sgmElement.inferTermType;
                    val subjectGraph = graphMapTermType match {
                        case Constants.R2RML_IRI_URI => { subjectGraphValue }
                        case _ => {
                            val errorMessage = "GraphMap's TermType is not valid: " + graphMapTermType;
                            logger.warn(errorMessage);
                            throw new MorphException(errorMessage);
                        }
                    }
                    subjectGraph
                });
                if (!subjectGraphs.isEmpty)
                    if (logger.isTraceEnabled()) logger.trace("Document " + i + " subject graphs: " + subjectGraphs)

                // Add subject resource to the JENA model with its class (rdf:type) and target graphs
                sm.classURIs.foreach(classURI => {
                    val classRes = this.materializer.model.createResource(classURI);
                    if (subjectGraphs == null || subjectGraphs.isEmpty) {
                        for (sub <- subjects) {
                            this.materializer.materializeQuad(sub, RDF.`type`, classRes, null);
                            this.materializer.outputStream.flush();
                        }
                    } else {
                        subjectGraphs.foreach(subjectGraph => {
                            for (sub <- subjects)
                                for (subG <- subjectGraph)
                                    this.materializer.materializeQuad(sub, RDF.`type`, classRes, subG);
                        });
                    }
                });

                // Internal loop on each predicate-object map
                poms.foreach(pom => {

                    // ----- Make a list of resources for the predicate maps of this predicate-object map
                    val predicates = pom.predicateMaps.flatMap(predicateMap => { this.translateData(predicateMap, document) });
                    if (logger.isDebugEnabled()) logger.debug("Document " + i + " predicates: " + predicates)

                    // ------ Make a list of resources for the object maps of this predicate-object map
                    val objects = pom.objectMaps.flatMap(objectMap => { this.translateData(objectMap, document) });
                    if (logger.isDebugEnabled()) logger.debug("Document " + i + " objects: " + objects)

                    // ----- For each RefObjectMap get the IRIs from the subject map of the parent triples map
                    val refObjects = pom.refObjectMaps.flatMap(refObjectMap => {

                        val parentTM = this.md.getParentTriplesMap(refObjectMap)

                        // Compute a list of subject IRIs for each of the join conditions, that we will intersect later
                        val parentSubjectsCandidates: Set[List[RDFNode]] = for (joinCond <- refObjectMap.joinConditions) yield {

                            // Evaluate the child reference on the current document (of the child triples map)
                            val childMsp = MixedSyntaxPath(joinCond.childRef, sm.refFormulaion)
                            val childValues: List[Object] = childMsp.evaluate(document)

                            // Evaluate the parent reference on the results of the parent triples map logical source
                            val parentValues = evalMixedSyntaxPathOnTriplesMap(parentTM, joinCond.parentRef)

                            // Make the actual join between the child values and parent values
                            val parentSubjects = parentValues.flatMap(parentResult => {
                                // For each document returned by the parent triples map (named parent document),
                                // if at least one of the child values is in the current parent document values, 
                                // then generate an RDF term for the subject of the current parent document.
                                if (!childValues.intersect(parentResult._2).isEmpty)
                                    Some(this.translateData(parentTM.subjectMap, parentResult._1))
                                else
                                    // There was no match: return an empty list so that the final intersection of candidate return nothing
                                    Some(List())
                            }).flatten
                            if (logger.isTraceEnabled()) logger.trace("Join parent candidates: " + joinCond.toString + ", result:" + parentSubjects)
                            parentSubjects
                        }

                        // There is a logical AND between several join conditions of the same RefObjectMap 
                        // => make the intersection between all subjects generated by all join conditions
                        val finalParentSubjects = GeneralUtility.intersectMultipleSets(parentSubjectsCandidates)
                        if (logger.isTraceEnabled()) logger.trace("Join parent subjects after intersection all joinConditions: " + finalParentSubjects)

                        // Optionally convert the result to an RDF collection or container
                        if (refObjectMap.isR2RMLTermType)
                            finalParentSubjects
                        else
                            createCollection(refObjectMap.termType.get, finalParentSubjects)
                    })
                    if (!refObjects.isEmpty)
                        if (logger.isDebugEnabled()) logger.debug("Document " + i + " refObjects: " + refObjects)

                    // ----- Create the list of resources representing target graphs mentioned in the predicate-object map
                    val pogm = pom.graphMaps;
                    val predicateObjectGraphs = pogm.map(pogmElement => {
                        val poGraphValue = this.translateData(pogmElement, document)
                        poGraphValue
                    });
                    if (!predicateObjectGraphs.isEmpty)
                        if (logger.isTraceEnabled()) logger.trace("Document" + i + " predicate-object map graphs: " + predicateObjectGraphs)

                    // ----- Finally, combine all the terms to generate triples in the target graphs or default graph
                    if (sm.graphMaps.isEmpty && pogm.isEmpty) {
                        predicates.foreach(predEl => {
                            objects.foreach(obj => {
                                for (sub <- subjects) {
                                    this.materializer.materializeQuad(sub, predEl, obj, null)
                                    if (logger.isDebugEnabled()) logger.debug("Materialized triple: [" + sub + "] [" + predEl + "] [" + obj + "]")
                                }
                            });

                            refObjects.foreach(obj => {
                                for (sub <- subjects) {
                                    if (obj != null) {
                                        this.materializer.materializeQuad(sub, predEl, obj, null)
                                        if (logger.isDebugEnabled()) logger.debug("Materialized triple: [" + sub + "] [" + predEl + "] [" + obj + "]")
                                    }
                                }
                            });
                        });
                    } else {
                        val unionGraphs = subjectGraphs ++ predicateObjectGraphs
                        unionGraphs.foreach(unionGraph => {
                            predicates.foreach(predEl => {
                                objects.foreach(obj => {
                                    unionGraphs.foreach(unionGraph => {
                                        for (sub <- subjects) {
                                            for (un <- unionGraph) {
                                                this.materializer.materializeQuad(sub, predEl, obj, un)
                                                if (logger.isDebugEnabled()) logger.debug("Materialized triple: graph[" + un + "], [" + sub + "] [" + predEl + "] [" + obj + "]")
                                            }
                                        }
                                    });
                                });

                                refObjects.foreach(obj => {
                                    for (sub <- subjects) {
                                        for (un <- unionGraph) {
                                            if (obj != null) {
                                                this.materializer.materializeQuad(sub, predEl, obj, un)
                                                if (logger.isDebugEnabled()) logger.debug("Materialized triple: graph[" + un + "], [" + sub + "] [" + predEl + "] [" + obj + "]")
                                            }
                                        }
                                    }
                                });
                            });
                        })
                    }
                });

            } catch {
                case e: MorphException => {
                    logger.error("Error while translating data of document " + i + ": " + e.getMessage);
                    e.printStackTrace()
                }
                case e: Exception => {
                    logger.error("Unexpected error while translating data of document " + i + ": " + e.getCause() + " - " + e.getMessage);
                    e.printStackTrace()
                }
            }
        }
        if (logger.isInfoEnabled()) System.out.println() // proper display of progress counter

        logger.info(i + " instances retrieved.");
    }

    /**
     * Apply a term map to a document of the result set, and generate a list of RDF terms:
     * for each element reference in the term map (reference or template), read values from the document,
     * then translate those values into RDF terms.
     */
    private def translateData(termMap: R2RMLTermMap, jsonDoc: String): List[RDFNode] = {

        var datatype = termMap.datatype
        var languageTag = termMap.languageTag

        // Term type of the collection/container to generate, or None if this is not the case 
        var collecTermType: Option[String] = None

        // Term type of the RDF terms to generate from database values
        var memberTermType: String = Constants.R2RML_LITERAL_URI

        // In case of a collection/container, a nested term map should give the details of term type, datatype and language or the terms 
        if (R2RMLTermMap.isRdfCollectionTermType(termMap.inferTermType)) {
            collecTermType = Some(termMap.inferTermType)
            if (termMap.nestedTermMap.isDefined) { // a nested term type MUST be defined in a term map with collection/container term type
                memberTermType = termMap.nestedTermMap.get.inferTermType
                datatype = termMap.nestedTermMap.get.datatype
                languageTag = termMap.nestedTermMap.get.languageTag
            } else
                logger.warn("Term map with collection/container term type but no nested term map: " + termMap)
        } else {
            collecTermType = None
            memberTermType = termMap.inferTermType
        }

        var result: List[RDFNode] = termMap.termMapType match {

            // --- Constant-valued term map
            case Constants.MorphTermMapType.ConstantTermMap => {
                this.translateSingleValue(termMap.constantValue, collecTermType, memberTermType, datatype, languageTag)
            }

            // --- Reference-valued term map
            case Constants.MorphTermMapType.ReferenceTermMap => {

                // Evaluate the value against the mixed syntax path
                val msPath = termMap.getMixedSyntaxPaths()(0) // '(0)' because in a reference there is only one mixed syntax path
                val values: List[Object] = msPath.evaluate(jsonDoc)

                // Generate RDF terms from the values resulting from the evaluation
                this.translateMultipleValues(values, collecTermType, memberTermType, datatype, languageTag)
            }

            // --- Template-valued term map
            case Constants.MorphTermMapType.TemplateTermMap => {

                // For each group of the template, compute a list of replacement strings
                val msPaths = termMap.getMixedSyntaxPaths()
                val listReplace = for (i <- 0 to (msPaths.length - 1)) yield {

                    // Evaluate the raw value against the mixed-syntax path.
                    val valuesRaw: List[Object] = msPaths(i).evaluate(jsonDoc)
                    valuesRaw.filter(_ != null)
                    //valuesRaw
                }

                val replacements: List[List[Object]] = listReplace.toList
                if (logger.isTraceEnabled()) logger.trace("Template replacements: " + replacements)

                // Check if at least one of the replacements is not null.
                var isEmptyReplacements: Boolean = true
                for (repl <- listReplace) {
                    if (!repl.isEmpty)
                        isEmptyReplacements = false
                }

                // Replace "{...}" groups in the template string with corresponding values from the db
                if (isEmptyReplacements) {
                    if (logger.isTraceEnabled()) logger.trace("Template " + termMap.templateString + ": no values (or only null values) were read from from the DB.")
                    List()
                } else {
                    // Compute the list of template results by making all possible combinations of the replacement values
                    val tplResults = TemplateUtility.replaceTemplateGroups(termMap.templateString, replacements);
                    this.translateMultipleValues(tplResults, collecTermType, memberTermType, datatype, languageTag)
                }
            }

            case _ => { throw new Exception("Invalid term map type " + termMap.termMapType) }
        }
        result
    }

    /**
     * Create a JENA literal resource with optional data type and language tag.
     * This method is overriden in the case of JSON to enable the mapping between JSON data types
     * and XSD data types
     */
    @throws[MorphException]
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
                val msg = "Error translating object uri value : " + value
                logger.error(msg);
                throw new MorphException(msg, e)
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

    /**
     * Execute a query against the database and apply an rml:iterator on the results.
     *
     * Results of the query are saved to an hash map, to avoid doing the same query several times
     * in case we need it again later. This is used to evaluate queries of triples maps and reuse
     * results for joins in case of reference object map.
     *
     * Major drawback: memory consumption, this is not appropriate for very big databases.
     */
    private def executeQueryAndIteraotr(query: GenericQuery, logSrcIterator: Option[String]): List[String] = {

        // A query is simply and uniquely identified by its concrete string value
        val queryMapId = query.concreteQuery.toString
        val queryResult =
            if (executedQueries.contains(queryMapId)) {
                executedQueries(queryMapId)
            } else {
                // Execute the query against the database, choose the execution method depending on the db type
                val resultSet = MongoUtils.execute(this.connection, query).toList

                // Save the result of this query in case it is asked again later (in a join)
                // @TODO USE WITH CARE: this would need to be strongly improved with the use of a real cache library,
                // and memory-consumption-based eviction.
                if (properties.cacheQueryResult)
                    executedQueries += (queryMapId -> resultSet)
                resultSet
            }

        // Apply the iterator to the result set, this creates a new result set
        val queryResultIter =
            if (logSrcIterator.isDefined) {
                val jPath = JSONPath_PathExpression.parseRaw(logSrcIterator.get)
                queryResult.flatMap(result => jPath.evaluate(result).map(value => value.toString))
            } else queryResult

        if (logger.isTraceEnabled()) logger.trace("Query returned " + queryResult.size + " results, " + queryResultIter.size + " result(s) after applying the iterator.")
        queryResultIter
    }

    /**
     * This method retrieves the data corresponding to a triples map, then
     * it evaluates an expression (mixed syntax path or a simple JSONPath expression) on
     * each result documents.
     * It is used to evaluate the parent reference of a join condition on the parent triples map.
     *
     * @param tm triples map
     * @param reference mixed syntax path expression to evaluate on the results of the triples map
     * @return a list of couples (result document, evaluation of the expression on that document).
     * The result document is one of the results of executing the logical source query.
     * The evaluation of each of these documents can return more than one value, thus the 2nd
     * member of the couple is a list.
     */
    private def evalMixedSyntaxPathOnTriplesMap(tm: R2RMLTriplesMap, reference: String): List[(String, List[Object])] = {

        // Get the query corresponding to that triples map
        var query: GenericQuery = null
        if (queries.contains(tm.id)) {
            logger.info("Query for triples map " + tm.id + ": " + queries(tm.id))
            query = queries(tm.id)
        } else {
            query = this.unfolder.unfoldConceptMapping(tm)
            queries += (tm.id -> query)
        }

        // Execute the query against the database
        val tmResultSet = executeQueryAndIteraotr(query, tm.logicalSource.docIterator)

        // Create the mixed syntax path corresponding to the reference
        val msp = MixedSyntaxPath(reference, tm.subjectMap.refFormulaion)

        // Evaluate each result of the triples map against the mixed syntax path
        val results = tmResultSet.map(tmResult => {
            (tmResult, msp.evaluate(tmResult))
        })

        results
    }
}
