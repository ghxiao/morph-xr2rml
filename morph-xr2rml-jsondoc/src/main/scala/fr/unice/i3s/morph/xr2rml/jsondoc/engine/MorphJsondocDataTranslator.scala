package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import java.sql.ResultSet

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFNode
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
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.path.JSONPath_PathExpression
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoUtils

class MorphJsondocDataTranslator(
    md: R2RMLMappingDocument,
    materializer: MorphBaseMaterializer,
    unfolder: MorphJsondocUnfolder,
    dataSourceReader: MorphBaseDataSourceReader,
    connection: GenericConnection, properties: MorphProperties)

        extends MorphBaseDataTranslator(md, materializer, unfolder, dataSourceReader, connection, properties)
        with MorphR2RMLElementVisitor {

    /** Store already executed queries do avoid running them several times */
    private var executedQueries: scala.collection.mutable.Map[String, List[String]] = new scala.collection.mutable.HashMap

    override val logger = Logger.getLogger(this.getClass().getName());

    override def generateRDFTriples(cm: MorphBaseClassMapping) = {
        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];

        val query = this.unfolder.unfoldConceptMapping(triplesMap);
        val logicalTable = triplesMap.logicalSource;
        val sm = triplesMap.subjectMap;
        val poms = triplesMap.predicateObjectMaps;

        this.generateRDFTriples(logicalTable, sm, poms, query);
    }

    /**
     * Query the database and build triples from the result. For each document of the result set:
     * (1) create a subject resource and an optional graph resource if the subject map contains a rr:graph/rr:graphMap property,
     * (2) loop on each predicate-object map: create a list of resources for the predicates, a list of resources for the objects,
     * a list of resources from the subject map of a parent object map in case there are referencing object maps,
     * and a list of resources representing target graphs mentioned in the predicate-object map.
     * (3) Finally combine all subject, graph, predicate and object resources to generate triples.
     */
    private def generateRDFTriples(logicalSrc: xR2RMLLogicalSource, sm: R2RMLSubjectMap, poms: Iterable[R2RMLPredicateObjectMap], query: GenericQuery) = {

        logger.info("Starting translating data into RDF instances...");
        if (sm == null) {
            val errorMessage = "No SubjectMap is defined";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }

        // Execute the query against the database and apply the iterator
        val resultSet = executeQueryAndIteraotr(query, logicalSrc.docIterator)

        // Main loop: iterate and process each result document of the result set
        var i = 0;
        for (document <- resultSet) {
            i = i + 1;
            logger.debug("Generating triples for document " + i + ": " + document)

            try {
                // Create the subject resource
                val subjects = this.translateData(sm, document)
                if (subjects == null) { throw new Exception("null value in the subject triple") }
                logger.debug("Document " + i + " subjects: " + subjects)

                // Create the list of resources representing subject target graphs
                val subjectGraphs = sm.graphMaps.map(sgmElement => {
                    val subjectGraphValue = this.translateData(sgmElement, document)
                    val graphMapTermType = sgmElement.inferTermType;
                    val subjectGraph = graphMapTermType match {
                        case Constants.R2RML_IRI_URI => { subjectGraphValue }
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
                    logger.debug("Document " + i + " predicates: " + predicates)

                    // ------ Make a list of resources for the object maps of this predicate-object map
                    val objects = pom.objectMaps.flatMap(objectMap => { this.translateData(objectMap, document) });
                    logger.debug("Document " + i + " objects: " + objects)

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
                            logger.trace("Join parent candidates: " + joinCond.toString + ", result:" + parentSubjects)
                            parentSubjects
                        }

                        // There is a logical AND between several join conditions of the same RefObjectMap 
                        // => make the intersection between all subjects generated by all join conditions
                        val finalParentSubjects = GeneralUtility.intersectMultipleSets(parentSubjectsCandidates)
                        logger.trace("Join parent subjects after intersection all joinConditions: " + finalParentSubjects)

                        // Optionally convert the result to an RDF collection or container
                        if (refObjectMap.isR2RMLTermType)
                            finalParentSubjects
                        else
                            List(createCollection(refObjectMap.termType.get, finalParentSubjects))
                    })
                    if (!refObjects.isEmpty)
                        logger.debug("Document " + i + " refObjects: " + refObjects)

                    // ----- Create the list of resources representing target graphs mentioned in the predicate-object map
                    val pogm = pom.graphMaps;
                    val predicateObjectGraphs = pogm.map(pogmElement => {
                        val poGraphValue = this.translateData(pogmElement, document)
                        poGraphValue
                    });
                    if (!predicateObjectGraphs.isEmpty)
                        logger.trace("Document" + i + " predicate-object map graphs: " + predicateObjectGraphs)

                    // ----- Finally, combine all the terms to generate triples in the target graphs or default graph
                    if (sm.graphMaps.isEmpty && pogm.isEmpty) {
                        predicates.foreach(predEl => {
                            objects.foreach(obj => {
                                for (sub <- subjects) {
                                    this.materializer.materializeQuad(sub, predEl, obj, null)
                                    logger.debug("Materialized triple: [" + sub + "] [" + predEl + "] [" + obj + "]")
                                }
                            });

                            refObjects.foreach(obj => {
                                for (sub <- subjects) {
                                    if (obj != null) {
                                        this.materializer.materializeQuad(sub, predEl, obj, null)
                                        logger.debug("Materialized triple: [" + sub + "] [" + predEl + "] [" + obj + "]")
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
                                                logger.debug("Materialized triple: graph[" + un + "], [" + sub + "] [" + predEl + "] [" + obj + "]")
                                            }
                                        }
                                    });
                                });

                                refObjects.foreach(obj => {
                                    for (sub <- subjects) {
                                        for (un <- unionGraph) {
                                            if (obj != null) {
                                                this.materializer.materializeQuad(sub, predEl, obj, un)
                                                logger.debug("Materialized triple: graph[" + un + "], [" + sub + "] [" + predEl + "] [" + obj + "]")
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
     * Apply a term map to a document of the result set, and generate a list of RDF terms:
     * for each element reference in the term map (reference or template), read values from the document,
     * translate those values into RDF terms.
     */
    private def translateData(termMap: R2RMLTermMap, jsonDoc: String): List[RDFNode] = {

        var result: List[RDFNode] = List();

        val dbType = this.properties.databaseType;
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val inferedTermType = termMap.inferTermType();

        result = termMap.termMapType match {

            // --- Constant-valued term map
            case Constants.MorphTermMapType.ConstantTermMap => {
                this.translateSingleValue(termMap.inferTermType, termMap.constantValue, termMap.datatype, termMap.languageTag)
            }

            // --- Reference-valued term map
            case Constants.MorphTermMapType.ReferenceTermMap => {

                // Evaluate the value against the mixed syntax path
                val msPath = termMap.getMixedSyntaxPaths()(0) // '(0)' because in a reference there is only one mixed syntax path
                val values: List[Object] = msPath.evaluate(jsonDoc)

                // Generate RDF terms from the values resulting from the evaluation
                this.translateMultipleValues(termMap.inferTermType, values, termMap.datatype, termMap.languageTag)
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

                // Check if at least one of the remplacements is not null.
                var isEmptyReplacements: Boolean = true
                for (repl <- listReplace) {
                    if (!repl.isEmpty)
                        isEmptyReplacements = false
                }

                // Replace "{...}" groups in the template string with corresponding values from the db
                if (isEmptyReplacements) {
                    logger.trace("Template " + termMap.templateString + ": no values read from from the DB.")
                    List()
                } else {
                    // Compute the list of template results by making all possible combinations of the replacement values
                    val tplResults = TemplateUtility.replaceTemplateGroups(termMap.templateString, replacements);
                    this.translateMultipleValues(termMap.inferTermType, tplResults, termMap.datatype, termMap.languageTag)
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
                val resultSet: List[String] = this.connection.dbType match {
                    case Constants.DatabaseType.MongoDB => { MongoUtils.execute(this.connection, query).toList }
                    case _ => { throw new Exception("Unsupported query type: should be an MongoDB query") }
                }

                // Save the result of this query in case it is asked again later
                executedQueries += (queryMapId -> resultSet)
                resultSet
            }

        // Apply the iterator to the result set, this creates a new result set
        val queryResultIter =
            if (logSrcIterator.isDefined) {
                val jPath = JSONPath_PathExpression.parseRaw(logSrcIterator.get)
                queryResult.flatMap(result => jPath.evaluate(result).map(value => value.toString))
            } else queryResult

        logger.trace("Query returned " + queryResult.size + " results, " + queryResultIter.size + " result(s) after applying the iterator.")
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
        val query = this.unfolder.unfoldConceptMapping(tm);

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
