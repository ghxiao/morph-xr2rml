package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
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
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
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
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoUtils
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader

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

        val resultSet: Iterator[String] = this.connection.dbType match {
            case Constants.DatabaseType.MongoDB => { MongoUtils.execute(this.connection, query) }
            case _ => { throw new Exception("Unsupported query type: should be an MongoDB query") }
        }

        // Make mappings of each column in the result set and its data type and equivalent XML data type
        /*
        var mapXMLDatatype: Map[String, String] = Map.empty;
        var mapDBDatatype: Map[String, Integer] = Map.empty;
        var rsmd: ResultSetMetaData = null;
        var columnCount = 0
        try {
            var rsmd = rows.getMetaData();
            val datatypeMapper = new DatatypeMapper();
            columnCount = rsmd.getColumnCount();
            for (i <- 0 until columnCount) {
                val columnName = rsmd.getColumnName(i + 1);
                val columnType = rsmd.getColumnType(i + 1);
                val mappedDatatype = datatypeMapper.getMappedType(columnType);
                mapXMLDatatype += (columnName -> mappedDatatype);
                mapDBDatatype += (columnName -> new Integer(columnType));
                logger.trace("SQL result: column " + columnName + ", mapped XML type: " + mappedDatatype)
            }
        } catch {
            case e: Exception => {
                logger.warn("Unable to detect database columns: " + e.getMessage());
            }
        } */

        // Main loop: iterate and process each result document of the result set
        var i = 0;
        while (resultSet.hasNext) {
            i = i + 1;
            val document = resultSet.next()
            logger.debug("Generating triples for row " + i + ": " + document)

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
     *  Create a JENA resource with an IRI after URL-encoding the string
     */
    private def createIRI(originalIRI: String) = {
        var resultIRI = originalIRI;
        try {
            resultIRI = GeneralUtility.encodeURI(resultIRI, properties.mapURIEncodingChars, properties.uriTransformationOperation);
            if (this.properties.encodeUnsafeChars) {
                resultIRI = GeneralUtility.encodeUnsafeChars(resultIRI);
            }
            if (this.properties.encodeReservedChars) {
                resultIRI = GeneralUtility.encodeReservedChars(resultIRI);
            }
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
    private def createLiteral(value: Object, datatype: Option[String], language: Option[String]): Literal = {
        try {
            val encodedValueAux =
                if (value == null) // case when the database returned NULL
                    ""
                else
                    GeneralUtility.encodeLiteral(value.toString())

            val encodedValue = encodedValueAux.toString();
            val data: String = datatype.getOrElse(null)
            val valueWithDataType = if (data != null) {
                val xsdDateTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
                val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();

                if (data.equals(xsdDateTimeURI)) {
                    this.translateDateTime(encodedValue);
                } else if (data.equals(xsdBooleanURI)) {
                    this.translateBoolean(encodedValue);
                } else { encodedValue }
            } else { encodedValue }

            val result: Literal = if (language.isDefined) {
                this.materializer.model.createLiteral(valueWithDataType, language.get);
            } else {
                if (datatype.isDefined) {
                    this.materializer.model.createTypedLiteral(valueWithDataType, datatype.get);
                } else {
                    this.materializer.model.createLiteral(valueWithDataType);
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

    private def translateDateTime(value: String) = {
        value.toString().trim().replaceAll(" ", "T");
    }

    private def translateBoolean(value: String) = {
        if (value.equalsIgnoreCase("T") || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1")) {
            "true";
        } else if (value.equalsIgnoreCase("F") || value.equalsIgnoreCase("False") || value.equalsIgnoreCase("0")) {
            "false";
        } else {
            "false";
        }
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

        println(values)
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
     * Apply a term map to the current row of the result set, and generate a list of RDF terms:
     * for each column reference in the term map (column, reference or template), read cell values from the current row,
     * translate them into one RDF term.
     * In the R2RML case, the result list should contain only one term.
     */
    private def translateData(termMap: R2RMLTermMap, jsonDoc: String): List[RDFNode] = {

        var result: List[RDFNode] = List(null);

        val dbType = this.properties.databaseType;
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val inferedTermType = termMap.inferTermType;

        result = termMap.termMapType match {

            // --- Constant-valued term map
            case Constants.MorphTermMapType.ConstantTermMap => {
                val datatype = if (termMap.datatype.isDefined) { termMap.datatype } else { None }
                this.translateSingleValue(termMap, termMap.constantValue, datatype)
            }

            // --- Reference-valued term map
            case Constants.MorphTermMapType.ReferenceTermMap => {

                // Parse reference as a mixed syntax path and return the column referenced in the first "Column()" path
                val colRef = termMap.getReferencedColumns().get(0)

                val colFromResultSet = {
                    // Match the column name in the term map definition with the column name in the result set  
                    if (logicalTableAlias != null && !"".equals(logicalTableAlias)) {
                        val termMapColSplit = colRef.split("\\.");
                        val columnName = termMapColSplit(termMapColSplit.length - 1).replaceAll("\"", dbEnclosedCharacter); ;
                        logicalTableAlias + "_" + columnName;
                    } else
                        colRef
                }

                // Read the value from the result set
                val dbValue = this.getResultSetValue(termMap, rs, colFromResultSet);

                // Evaluate the value against the mixed syntax path
                val msPath = termMap.getMixedSyntaxPaths()(0)
                val values: List[Object] = msPath.evaluate(dbValue)

                // Generate RDF terms from the values
                val datatype =
                    if (termMap.datatype.isDefined)
                        termMap.datatype
                    else
                        mapXMLDatatype.get(colRef.replaceAll("\"", ""))
                this.translateMultipleValues(termMap, values, datatype);
            }

            // --- Template-valued term map
            case Constants.MorphTermMapType.TemplateTermMap => {

                val datatype = if (termMap.datatype.isDefined) { termMap.datatype } else { None }

                // For each group of the template, compute a list of replacement strings

                val colRefs = termMap.getReferencedColumns()
                val msPaths = termMap.getMixedSyntaxPaths()

                val listReplace = for (i <- 0 to (colRefs.length - 1)) yield {

                    // Match the column name in the term map definition with the column name in the result set  
                    val colRef = colRefs(i)
                    val colFromResultSet =
                        if (logicalTableAlias != null) {
                            val termMapColSplit = colRef.split("\\.");
                            val columnName = termMapColSplit(termMapColSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);
                            logicalTableAlias + "_" + columnName;
                        } else
                            colRef

                    // Read the value from the result set
                    val dbValueRaw = this.getResultSetValue(termMap, rs, colFromResultSet)

                    // Evaluate the raw value against the mixed-syntax path.
                    // If the reference is not a mixed-syntax path, the value is simply returned in a List()
                    val valuesRaw: List[Object] = msPaths(i).evaluate(dbValueRaw)
                    valuesRaw.filter(_ != null)
                }

                val replacements: List[List[Object]] = listReplace.toList
                logger.trace("Template replacements: " + replacements)

                // Replace {} groups in the template string with corresponding values from the db
                if (replacements.isEmpty) {
                    logger.warn("Template " + termMap.templateString + ": no group to replace with values from the DB.")
                    null
                } else {
                    // Compute the list of template results by making all possible combinations of the replacement values
                    val templates = TemplateUtility.replaceTemplateGroups(termMap.templateString, replacements);
                    this.translateMultipleValues(termMap, templates, datatype);
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