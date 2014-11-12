package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.RDFList
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.vocabulary.RDF
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
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
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath

class MorphRDBDataTranslator(
    md: R2RMLMappingDocument,
    materializer: MorphBaseMaterializer,
    unfolder: MorphRDBUnfolder,
    dataSourceReader: MorphRDBDataSourceReader,
    connection: Connection, properties: MorphProperties)

        extends MorphBaseDataTranslator(md, materializer, unfolder, dataSourceReader, connection, properties)
        with MorphR2RMLElementVisitor {

    override val logger = Logger.getLogger(this.getClass().getName());

    override def processCustomFunctionTransformationExpression(argument: Object): Object = {
        null;
    }

    override def translateData(triplesMap: MorphBaseClassMapping): Unit = {
        val query = this.unfolder.unfoldConceptMapping(triplesMap);
        this.generateRDFTriples(triplesMap, query);
        // null;
    }

    override def translateData(triplesMaps: Iterable[MorphBaseClassMapping]): Unit = {
        for (triplesMap <- triplesMaps) {
            try {
                this.visit(triplesMap.asInstanceOf[R2RMLTriplesMap]);
            } catch {
                case e: Exception => {
                    logger.error("error while translating data of triplesMap : " + triplesMap);
                    if (e.getMessage() != null) {
                        logger.error("error message = " + e.getMessage());
                    }
                    throw new Exception(e.getMessage(), e);
                }
            }
        }
    }

    override def translateData(mappingDocument: MorphBaseMappingDocument) = {
        val conn = this.connection
        val triplesMaps = mappingDocument.classMappings
        if (triplesMaps != null) {
            this.translateData(triplesMaps);
            //DBUtility.closeConnection(conn, "R2RMLDataTranslator");
        }
    }

    def visit(logicalTable: xR2RMLLogicalSource): Object = {
        // TODO Auto-generated method stub
        null;
    }

    override def visit(mappingDocument: R2RMLMappingDocument): Object = {
        try {
            this.translateData(mappingDocument);
        } catch {
            case e: Exception => {
                e.printStackTrace();
                logger.error("error during data translation process : " + e.getMessage());
                throw new Exception(e.getMessage());
            }
        }
        null;
    }

    def visit(objectMap: R2RMLObjectMap): Object = {
        // TODO Auto-generated method stub
        null;
    }

    def visit(refObjectMap: R2RMLRefObjectMap): Object = {
        // TODO Auto-generated method stub
        null;
    }

    def visit(r2rmlTermMap: R2RMLTermMap): Object = {
        // TODO Auto-generated method stub
        null;
    }

    def visit(triplesMap: R2RMLTriplesMap): Object = {
        this.translateData(triplesMap);
        null;
    }

    /**
     * Query the database and build triples from the result. For each row of the result set,
     * (1) create a subject resource and an optional graph resource if the subject map contains a rr:graph/rr:graphMap property,
     * (2) loop on each predicate-object map: create a list of resources for the predicates, a list of resources for the objects,
     * a list of resources from the subject map of a parent object map in case there are referencing object maps,
     * and a list of resources representing target graphs mentioned in the predicate-object map.
     * (3) Finally combine all subject, graph, predicate and object resources to generate triples.
     */
    def generateRDFTriples(logicalTable: xR2RMLLogicalSource, sm: R2RMLSubjectMap, poms: Iterable[R2RMLPredicateObjectMap], iQuery: IQuery) = {

        logger.debug("generateRDFTriples: starting translating RDB data into RDF instances...");
        if (sm == null) {
            val errorMessage = "No SubjectMap is defined";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }

        // Run the query against the database 
        val rows = DBUtility.execute(this.connection, iQuery.toString(), this.properties.databaseTimeout);
        // logger.trace(DBUtility.resultSetToString(rows)) // debug only, can't be reset afterwards

        // Make mappings of each column in the result set and its data type and equivalent XML data type
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
        }

        // Main loop: iterate and process each row in the SQL result set 
        var i = 0;
        while (rows.next()) { // put current cursor on the new row
            i = i + 1;
            logger.debug("Generating triples for row " + i + ": " + DBUtility.resultSetCurrentRowToString(rows))
            try {

                // Create the subject resource
                val subject = this.translateData(sm, rows, logicalTable.alias, mapXMLDatatype);
                if (subject == null) { throw new Exception("null value in the subject triple") }
                logger.trace("Row " + i + " subjects: " + subject)

                // Create the list of resources representing subject target graphs
                val subjectGraphs = sm.graphMaps.map(sgmElement => {
                    val subjectGraphValue = this.translateData(sgmElement, rows, logicalTable.alias, mapXMLDatatype)
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
                    logger.trace("Row " + i + " subject graphs: " + subjectGraphs)

                // Add subject resource to the JENA model with its class (rdf:type) and target graphs
                sm.classURIs.foreach(classURI => {
                    val statementObject = this.materializer.model.createResource(classURI);
                    if (subjectGraphs == null || subjectGraphs.isEmpty) {
                        for (sub <- subject) {
                            this.materializer.materializeQuad(sub, RDF.`type`, statementObject, null);
                            this.materializer.outputStream.flush();
                        }
                    } else {
                        subjectGraphs.foreach(subjectGraph => {
                            for (sub <- subject) {
                                for (subG <- subjectGraph) {
                                    this.materializer.materializeQuad(sub, RDF.`type`, statementObject, subG);
                                }
                            }
                        });
                    }
                });

                // Internal loop on each predicate-object map
                poms.foreach(pom => {

                    val alias = if (pom.getAlias() == null) { logicalTable.alias; }
                    else { pom.getAlias() }

                    // Make a list of resources for the predicate maps of this predicate-object map
                    val predicates = pom.predicateMaps.map(predicateMap => {
                        this.translateData(predicateMap, rows, logicalTable.alias, mapXMLDatatype)
                    });
                    logger.trace("Row " + i + " predicates: " + predicates)

                    // Make a list of resources for the object maps of this predicate-object map
                    val objects = pom.objectMaps.map(objectMap => {
                        this.translateData(objectMap, rows, alias, mapXMLDatatype)
                    });
                    logger.trace("Row " + i + " objects: " + objects)

                    // In case of a ReferencingObjectMaps, get the object IRI from the subject map of the parent triples map  
                    val refObjects = pom.refObjectMaps.map(refObjectMap => {
                        val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap)
                        val parentSubjectMap = parentTriplesMap.subjectMap;
                        val parentTableAlias = this.unfolder.mapRefObjectMapAlias.getOrElse(refObjectMap, null);
                        val parentSubjects = this.translateData(parentSubjectMap, rows, parentTableAlias, mapXMLDatatype)
                        parentSubjects

                        /* if (xR2RMLDataTranslator.checkJoinParseCondition(refObjectMap, rows, this.properties.databaseType, parentTableAlias, logicalTable.alias)) {
                            val parentSubjectMap = parentTriplesMap.subjectMap;
                            //because of the fact that the treatment is row per row, i haven't find yet a way to gather all the subjects that constitute the list
                            //  val parentSubjects = this.translaterefObjectData(parentSubjectMap, rows, parentTableAlias, mapXMLDatatype,defaultFormt,refTermtype)
                            parentSubjects
                        } else {
                            null
                        } */
                    })
                    if (!refObjects.isEmpty)
                        logger.trace("Row " + i + " refObjects: " + refObjects)

                    // Create the list of resources representing target graphs mentioned in the predicate-object map
                    val pogm = pom.graphMaps;
                    val predicateObjectGraphs = pogm.map(pogmElement => {
                        val poGraphValue = this.translateData(pogmElement, rows, null, mapXMLDatatype)
                        poGraphValue
                    });
                    if (!predicateObjectGraphs.isEmpty)
                        logger.trace("Row " + i + " predicate-object map graphs: " + predicateObjectGraphs)

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
        rows.close();
    }

    override def generateRDFTriples(cm: MorphBaseClassMapping, iQuery: IQuery) = {
        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
        val logicalTable = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val sm = triplesMap.subjectMap;
        val poms = triplesMap.predicateObjectMaps;
        this.generateRDFTriples(logicalTable, sm, poms, iQuery);
    }

    override def generateSubjects(cm: MorphBaseClassMapping, iQuery: IQuery) = {
        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
        val logicalTable = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val sm = triplesMap.subjectMap;
        this.generateRDFTriples(logicalTable, sm, Nil, iQuery);
    }

    /**
     *  Create a JENA resource with an IRI after URL-encoding the string
     */
    def createIRI(originalIRI: String) = {
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
    def createLiteral(value: Object, datatype: Option[String], language: Option[String]): Literal = {
        try {
            //val encodedValueAux = GeneralUtility.encodeLiteral(value.toString());

            val encodedValue = value.toString();
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

    def translateDateTime(value: String) = {
        value.toString().trim().replaceAll(" ", "T");
    }

    def translateBoolean(value: String) = {
        if (value.equalsIgnoreCase("T") || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1")) {
            "true";
        } else if (value.equalsIgnoreCase("F") || value.equalsIgnoreCase("False") || value.equalsIgnoreCase("0")) {
            "false";
        } else {
            "false";
        }
    }

    /**
     * Create a list of RDF terms (as JENA resources) from a value read from the database,
     * depending on the term type specified in the term map.
     * For the R2RML case, the list contains only one RDF term.
     */
    def translateData(termMap: R2RMLTermMap, dbValue: Object, datatype: Option[String]): List[RDFNode] = {

        var resultat: List[RDFNode] = List(null);
        resultat = termMap.inferTermType match {

            case Constants.R2RML_IRI_URI => {
                if (dbValue != null) {
                    var result = List(this.createIRI(dbValue.toString()));
                    //for (un <- result) { xR2RMLJsonUtils.saveNode(un) }
                    result
                } else { null }
            }

            case Constants.R2RML_LITERAL_URI => {
                if (dbValue != null) {
                    var result = List(this.createLiteral(dbValue, datatype, termMap.languageTag));
                    //for (un <- result) { xR2RMLJsonUtils.saveNode(un) }
                    result
                } else { null }
            }

            case Constants.R2RML_BLANKNODE_URI => {
                if (dbValue != null) {
                    var rep = GeneralUtility.encodeReservedChars(GeneralUtility.encodeUnsafeChars(dbValue.toString()))
                    var result = List(this.materializer.model.createResource(new AnonId(rep)));
                    //for (un <- result) { xR2RMLJsonUtils.saveNode(un) }
                    result
                } else { null }
            }

            case xR2RML_Constants.xR2RML_RDFLIST_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, termMap.languageTag);
                    val list = ModelFactory.createDefaultModel().createList(tab)
                    var result = List(list)
                    result
                } else { null }
            }

            case xR2RML_Constants.xR2RML_RDFBAG_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, termMap.languageTag);
                    val bag = ModelFactory.createDefaultModel().createBag()
                    bag.add(tab(0))
                    var result = List(bag)
                    result
                } else { null }
            }

            case xR2RML_Constants.xR2RML_RDFALT_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, termMap.languageTag);
                    val alt = ModelFactory.createDefaultModel().createAlt()
                    alt.add(tab(0))
                    var result = List(alt)
                    result
                } else { null }
            }

            case xR2RML_Constants.xR2RML_RDFSEQ_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, termMap.languageTag);
                    val seq = ModelFactory.createDefaultModel().createSeq()
                    seq.add(tab(0))
                    var result = List(seq)
                    result
                } else { null }
            }

            case _ => { throw new Exception("Unkown term type: " + termMap.inferTermType) }
        }

        logger.trace("    Translated value [" + dbValue + " ] into [" + resultat + "]")
        resultat
    }

    /**
     * Apply a term map to the current row of the result set, and generate a list of RDF terms:
     * for each column reference in the term map (column, reference or template), read cell values from the current row,
     * translate them into one RDF term.
     * In the R2RML case, the result list should contain only one term.
     */
    def translateData(termMap: R2RMLTermMap, rs: ResultSet, logicalTableAlias: String, mapXMLDatatype: Map[String, String]): List[RDFNode] = {

        var result: List[RDFNode] = List(null);

        val dbType = this.properties.databaseType;
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val inferedTermType = termMap.inferTermType;

        result = termMap.termMapType match {

            // --- Constant-valued term map
            case Constants.MorphTermMapType.ConstantTermMap => {
                val datatype = if (termMap.datatype.isDefined) { termMap.datatype } else { None }
                this.translateData(termMap, termMap.constantValue, datatype);
            }

            // --- Column-valued term map
            case Constants.MorphTermMapType.ColumnTermMap => {
                // Match the column name in the term map definition with the column name in the result set  
                val columnTermMapValue =
                    if (logicalTableAlias != null && !logicalTableAlias.equals("")) {
                        val termMapColumnValueSplit = termMap.columnName.split("\\.");
                        val columnName = termMapColumnValueSplit(termMapColumnValueSplit.length - 1).replaceAll("\"", dbEnclosedCharacter); ;
                        logicalTableAlias + "_" + columnName;
                    } else { termMap.columnName }

                // Read the value from the result set and get its XML datatype
                val dbValue = this.getResultSetValue(termMap, rs, columnTermMapValue);
                val datatype =
                    if (termMap.datatype.isDefined) { termMap.datatype }
                    else {
                        val columnNameAux = termMap.columnName.replaceAll("\"", "");
                        val datatypeAux = mapXMLDatatype.get(columnNameAux)
                        datatypeAux
                    }

                // Generate the RDF terms
                this.translateData(termMap, dbValue, datatype);
            }

            // --- Reference-valued term map
            case Constants.MorphTermMapType.ReferenceTermMap => {

                // Match the column name in the term map definition with the column name in the result set  
                val columnTermMapValue = {
                    // Parse reference as a mixed syntax path and return the column referenced in the first path "Column()"
                    val colRef = termMap.getReferencedColumns().get(0)

                    if (logicalTableAlias != null && !"".equals(logicalTableAlias)) {
                        val termMapColumnValueSplit = colRef.split("\\.");
                        val columnName = termMapColumnValueSplit(termMapColumnValueSplit.length - 1).replaceAll("\"", dbEnclosedCharacter); ;
                        logicalTableAlias + "_" + columnName;
                    } else
                        colRef
                }

                // Read the value from the result set and get its XML datatype
                val dbValue = this.getResultSetValue(termMap, rs, columnTermMapValue);
                val datatype =
                    if (termMap.datatype.isDefined) { termMap.datatype }
                    else {
                        val columnNameAux = termMap.reference.replaceAll("\"", "");
                        val datatypeAux = mapXMLDatatype.get(columnNameAux)
                        datatypeAux
                    }

                // Generate the RDF terms
                this.translateData(termMap, dbValue, datatype);
            }

            // --- Template-valued term map
            case Constants.MorphTermMapType.TemplateTermMap => {

                val datatype = if (termMap.datatype.isDefined) { termMap.datatype } else { None }

                // Process each column referenced in the template: compute a list of replacements, 
                // namely the values to replace with 'column' groups in the template string
                val colNames = termMap.getReferencedColumns()

                val replacements: Map[String, String] = colNames.map(colName =>
                    {
                        // Match the column name in the term map definition with the column name in the result set  
                        val databaseColumn =
                            if (logicalTableAlias != null) {
                                val attributeSplit = colName.split("\\.");
                                if (attributeSplit.length >= 1) {
                                    val columnName = attributeSplit(attributeSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);
                                    logicalTableAlias + "_" + columnName;
                                } else { logicalTableAlias + "_" + colName; }
                            } else { colName; }

                        // Read the value from the result set
                        val dbValueAux = this.getResultSetValue(termMap, rs, databaseColumn);
                        val dbValue = dbValueAux match {
                            // If the value read from the DB is a string, then optionally transform it as configured
                            case dbValueAuxString: String => {
                                if (this.properties.transformString.isDefined) {
                                    this.properties.transformString.get match {
                                        case Constants.TRANSFORMATION_STRING_TOLOWERCASE => { dbValueAuxString.toLowerCase(); }
                                        case Constants.TRANSFORMATION_STRING_TOUPPERCASE => { dbValueAuxString.toUpperCase(); }
                                        case _ => { dbValueAuxString }
                                    }
                                } else { dbValueAuxString }
                            }
                            case _ => { dbValueAux }
                        }

                        // Optionally, it the term map term type is IRI, then transform URI characters (lowercase, uppercase)
                        if (dbValue != null) {
                            var databaseValueString = dbValue.toString();
                            if (termMap.inferTermType.equals(Constants.R2RML_IRI_URI)) {
                                val uriTransformationOperations = this.properties.uriTransformationOperation;
                                if (uriTransformationOperations != null) {
                                    uriTransformationOperations.foreach {
                                        case Constants.URI_TRANSFORM_TOLOWERCASE => { databaseValueString = databaseValueString.toLowerCase(); }
                                        case Constants.URI_TRANSFORM_TOUPPERCASE => { databaseValueString = databaseValueString.toUpperCase(); }
                                        case _ => {}
                                    }
                                }
                            }
                            (colName -> databaseValueString)
                        } else (colName -> "")
                    }).toMap
                logger.trace("Template replacements: " + replacements)

                // Replace {} groups in the template string with corresponding values from the db
                if (replacements.isEmpty) {
                    logger.warn("Template " + termMap.templateString + ": no group to replace with values from the DB.")
                    null
                } else {
                    val templateWithDBValue = RegexUtility.replaceTemplateTokens(termMap.templateString, replacements);
                    this.translateData(termMap, templateWithDBValue, datatype);
                }
            }

            case _ => { throw new Exception("Invalid term map type " + termMap.termMapType) }
        }
        result
    }

    def getResultSetValue(termMap: R2RMLTermMap, rs: ResultSet, pColumnName: String): Object = {
        try {
            val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME);
            val tableName = zConstant.table;
            val columnNameAux = zConstant.column
            val columnName = {
                if (tableName != null) {
                    tableName + "." + columnNameAux
                } else {
                    columnNameAux
                }
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
                logger.error("error occured when translating result: " + e.getMessage());
                null
            }
        }
    }
}