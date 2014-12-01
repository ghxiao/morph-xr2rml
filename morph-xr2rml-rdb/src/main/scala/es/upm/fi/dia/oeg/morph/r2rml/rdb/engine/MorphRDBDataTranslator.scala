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
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
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
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery

class MorphRDBDataTranslator(
    md: R2RMLMappingDocument,
    materializer: MorphBaseMaterializer,
    unfolder: MorphRDBUnfolder,
    dataSourceReader: MorphRDBDataSourceReader,
    genCnx: GenericConnection, properties: MorphProperties)

        extends MorphBaseDataTranslator(md, materializer, unfolder, dataSourceReader, genCnx, properties)
        with MorphR2RMLElementVisitor {

    if (!genCnx.isRelationalDB)
        throw new Exception("Database connection type does not mathc relational database")

    private val connection = genCnx.concreteCnx.asInstanceOf[Connection]

    override val logger = Logger.getLogger(this.getClass().getName());

    override def generateRDFTriples(cm: MorphBaseClassMapping) = {
        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];

        val query = this.unfolder.unfoldConceptMapping(triplesMap);
        if (!query.isSqlQuery)
            throw new Exception("Unsupported query type: should be an SQL query")
        val logicalTable = triplesMap.logicalSource;
        val sm = triplesMap.subjectMap;
        val poms = triplesMap.predicateObjectMaps;

        this.generateRDFTriples(logicalTable, sm, poms, query.concreteQuery.asInstanceOf[IQuery]);
    }

    /**
     * Query the database and build triples from the result. For each row of the result set,
     * (1) create a subject resource and an optional graph resource if the subject map contains a rr:graph/rr:graphMap property,
     * (2) loop on each predicate-object map: create a list of resources for the predicates, a list of resources for the objects,
     * a list of resources from the subject map of a parent object map in case there are referencing object maps,
     * and a list of resources representing target graphs mentioned in the predicate-object map.
     * (3) Finally combine all subject, graph, predicate and object resources to generate triples.
     */
    private def generateRDFTriples(logicalTable: xR2RMLLogicalSource, sm: R2RMLSubjectMap, poms: Iterable[R2RMLPredicateObjectMap], iQuery: IQuery) = {

        logger.info("Starting translating RDB data into RDF instances...");
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
                logger.trace("SQL result: column " + columnName + " is mapped to XML type: " + mappedDatatype)
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
            logger.debug("Row " + i + ": " + DBUtility.resultSetCurrentRowToString(rows))
            try {
                // Create the subject resource
                val subjects = this.translateData(sm, rows, logicalTable.alias, mapXMLDatatype);
                if (subjects == null) { throw new Exception("null value in the subject triple") }
                logger.debug("Row " + i + " subjects: " + subjects)

                // Create the list of resources representing subject target graphs
                val subjectGraphs = sm.graphMaps.map(sgmElement => {
                    val subjectGraphValue = this.translateData(sgmElement, rows, logicalTable.alias, mapXMLDatatype)
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
                    logger.trace("Row " + i + " subject graphs: " + subjectGraphs)

                // Add subject resource to the JENA model with its class (rdf:type) and target graphs
                sm.classURIs.foreach(classURI => {
                    val classRes = this.materializer.model.createResource(classURI);
                    if (subjectGraphs == null || subjectGraphs.isEmpty) {
                        for (sub <- subjects)
                            this.materializer.materializeQuad(sub, RDF.`type`, classRes, null);
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

                    val alias = if (pom.alias == null) { logicalTable.alias; }
                    else { pom.alias }

                    // ----- Make a list of resources for the predicate maps of this predicate-object map
                    val predicates = pom.predicateMaps.flatMap(predicateMap => {
                        this.translateData(predicateMap, rows, logicalTable.alias, mapXMLDatatype)
                    });
                    logger.debug("Row " + i + " predicates: " + predicates)

                    // ----- Make a list of resources for the object maps of this predicate-object map
                    val objects = pom.objectMaps.flatMap(objectMap => {
                        this.translateData(objectMap, rows, alias, mapXMLDatatype)
                    });
                    logger.debug("Row " + i + " objects: " + objects)

                    // ----- For each RefObjectMap get the IRIs from the subject map of the parent triples map

                    /* ####################################################################################
                     * Need to update treatment of ReferencingObjectMaps in xR2RML context
                     * ####################################################################################
                     */
                    val refObjects = pom.refObjectMaps.flatMap(refObjectMap => {
                        val parentTM = this.md.getParentTriplesMap(refObjectMap)
                        val parentTabAlias = this.unfolder.mapRefObjectMapAlias.getOrElse(refObjectMap, null);
                        val parentSubjects = this.translateData(parentTM.subjectMap, rows, parentTabAlias, mapXMLDatatype)
                        parentSubjects

                        /* if (xR2RMLDataTranslator.checkJoinParseCondition(refObjectMap, rows, this.properties.databaseType, parentTabAlias, logicalTable.alias)) {
                            val parentSubjectMap = parentTM.subjectMap;
                            //because of the fact that the treatment is row per row, i haven't find yet a way to gather all the subjects that constitute the list
                            //  val parentSubjects = this.translaterefObjectData(parentSubjectMap, rows, parentTabAlias, mapXMLDatatype,defaultFormt,refTermtype)
                            parentSubjects
                        } else {
                            null
                        } */
                    })
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
        rows.close();
    }

    /**
     * Apply a term map to the current row of the result set, and generate a list of RDF terms:
     * for each column reference in the term map (column, reference or template), read cell values from the current row,
     * translate them into one RDF term.
     * In the R2RML case, the result list should contain only one term.
     */
    private def translateData(termMap: R2RMLTermMap, rs: ResultSet, logicalTableAlias: String, mapXMLDatatype: Map[String, String]): List[RDFNode] = {

        var result: List[RDFNode] = List();

        val dbType = this.properties.databaseType;
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val inferedTermType = termMap.inferTermType;

        result = termMap.termMapType match {

            // --- Constant-valued term map
            case Constants.MorphTermMapType.ConstantTermMap => {
                this.translateSingleValue(termMap.inferTermType, termMap.constantValue, termMap.datatype, termMap.languageTag)
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
                this.translateSingleValue(termMap.inferTermType, dbValue, termMap.datatype, termMap.languageTag)
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
                    else {
                        val dt = mapXMLDatatype.get(colRef.replaceAll("\"", ""))
                        if (dt.isDefined && dt.get == null)
                            None
                        else dt
                    }
                this.translateMultipleValues(termMap.inferTermType, values, termMap.datatype, termMap.languageTag)
            }

            // --- Template-valued term map
            case Constants.MorphTermMapType.TemplateTermMap => {

                val colRefs = termMap.getReferencedColumns()
                val msPaths = termMap.getMixedSyntaxPaths()

                // For each group of the template, compute a list of replacement strings: if the db returns null,
                // then return an empty list as replacement. We must return a list of replacements, possibly empty replacements,
                // but as many replacements as groups {} in the template string.

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
                    // If null was returned, then return an empty list.
                    val valuesRaw: List[Object] = msPaths(i).evaluate(dbValueRaw)
                    valuesRaw.filter(_ != null)
                }

                val replacements: List[List[Object]] = listReplace.toList
                logger.trace("Template replacements: " + replacements)

                // Check if at least one of the remplacements is not null.
                var isEmptyReplacements: Boolean = true
                for (repl <- listReplace) {
                    if (!repl.isEmpty)
                        isEmptyReplacements = false
                }

                // Replace {} groups in the template string with corresponding values from the db               
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

    def visit(logicalTable: xR2RMLLogicalSource): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(mappingDocument: R2RMLMappingDocument): Object = {
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