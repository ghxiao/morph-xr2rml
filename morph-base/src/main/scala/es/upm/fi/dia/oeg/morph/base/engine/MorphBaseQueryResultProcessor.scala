package es.upm.fi.dia.oeg.morph.base.engine

import java.io.OutputStream
import java.io.Writer
import com.hp.hpl.jena.query.Query
import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.JavaConversions.setAsJavaSet
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import java.util.regex.Pattern
import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import java.util.regex.Matcher
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator
import es.upm.fi.dia.oeg.morph.base.sql.IQuery

abstract class MorphBaseQueryResultProcessor(
        var mappingDocument: R2RMLMappingDocument,
        var properties: MorphProperties,
        var outputStream: Writer,
        var dataSourceReader: MorphBaseDataSourceReader) {

    var prSQLGenerator: MorphBasePRSQLGenerator = null

    private var mapTemplateMatcher: Map[String, Matcher] = Map.empty;

    private var mapTemplateAttributes: Map[String, List[String]] = Map.empty;

    val logger = Logger.getLogger(this.getClass());

    /**
     * Initialize the XML tree for the SPARQL result set with one variable node
     * for each projected variable in  the SPARQL query
     * <pre>
     * 	<sparql>
     *  	<head>
     *   		</variable name="var1>
     *   		</variable name="var2>
     *     		...
     *  	</head>
     * 	</sparql>
     * </pre>
     */
    def preProcess(sparqlQuery: Query): Unit;

    /**
     * Create the body of the SPARQL result set
     * <pre>
     * <result>
     * 	 <binding name="var1"> <uri>...</uri> </binding>
     * 	 <binding name="var2"> <literal>...</literal> </binding>
     *   ...
     * </result>
     * ...
     * </pre>
     *
     */
    def process(sparqlQuery: Query, resultSet: MorphBaseResultSet): Unit;

    /** Save the XML document to a file */
    def postProcess(): Unit;

    def getOutput(): Object;

    def translateResult(mapSparqlSql: Map[Query, IQuery]) {
        val start = System.currentTimeMillis();
        var i = 0;
        mapSparqlSql.foreach(mapElement => {
            val sparqlQuery = mapElement._1
            val iQuery = mapElement._2

            // Execution of the concrete SQL query against the database
            val abstractResultSet = this.dataSourceReader.execute(iQuery.toString());
            val columnNames = iQuery.getSelectItemAliases();
            abstractResultSet.setColumnNames(columnNames);

            // Write the XMl result set to the output
            if (i == 0) {
                // The first time, initialize the XML document of the SPARQL result set
                this.preProcess(sparqlQuery);
            }
            this.process(sparqlQuery, abstractResultSet);
            i = i + 1;
        })

        if (i > 0) {
            this.postProcess();
        }

        val end = System.currentTimeMillis();
        logger.info("Result generation time = " + (end - start) + " ms.");
    }

    protected def translateResultSet(varName: String, rs: MorphBaseResultSet): TermMapResult = {
        val result: TermMapResult = {
            try {
                if (rs != null) {
                    // Retrieve the columns from the result set, that match this SPARQL variable
                    val columnNames = this.getElementsStartWith(rs.getColumnNames(), varName + "_");

                    // Retrieve the values, from the result set, that match this SPARQL variable
                    val mapValue = this.getMappedMappingByVarName(varName, rs);

                    if (!mapValue.isDefined) {
                        val originalValue = rs.getString(varName);
                        new TermMapResult(originalValue, null, None)
                    } else {
                        val termMap: R2RMLTermMap = {
                            mapValue.get match {
                                case mappedValueTermMap: R2RMLTermMap => {
                                    mappedValueTermMap;
                                }
                                case mappedValueRefObjectMap: R2RMLRefObjectMap => {
                                    val parentTriplesMap = this.mappingDocument.getParentTriplesMap(mappedValueRefObjectMap);
                                    parentTriplesMap.subjectMap;
                                }
                                case _ => {
                                    logger.error("Undefined term map type");
                                    null
                                }
                            }
                        }

                        val resultAux = {
                            if (termMap != null) {
                                termMap.termMapType match {
                                    case Constants.MorphTermMapType.TemplateTermMap => {
                                        val templateString = termMap.getTemplateString();
                                        if (this.mapTemplateMatcher.contains(templateString)) {
                                            val matcher = this.mapTemplateMatcher.get(templateString);
                                            // so what??? This block does nothing
                                        } else {
                                            val pattern = Pattern.compile(Constants.R2RML_TEMPLATE_PATTERN);
                                            val matcher = pattern.matcher(templateString);
                                            this.mapTemplateMatcher += (templateString -> matcher);
                                        }

                                        val templateAttributes = {
                                            if (this.mapTemplateAttributes.contains(templateString)) {
                                                this.mapTemplateAttributes(templateString);
                                            } else {
                                                val templateAttributesAux = TemplateUtility.getTemplateColumns(templateString);
                                                this.mapTemplateAttributes += (templateString -> templateAttributesAux);
                                                templateAttributesAux;
                                            }
                                        }

                                        var i = 0;
                                        val replaceMentAux = templateAttributes.map(templateAttribute => {
                                            val columnName = {
                                                if (columnNames == null || columnNames.isEmpty()) {
                                                    varName;
                                                } else {
                                                    varName + "_" + i;
                                                }
                                            }
                                            i = i + 1;

                                            val dbValue = rs.getString(columnName);
                                            templateAttribute -> dbValue;
                                        })
                                        val replacements = replaceMentAux.toMap;

                                        val templateResult = if (replacements.size() > 0) {
                                            TemplateUtility.replaceTemplateGroups(templateString, List(List(replacements)))
                                        } else {
                                            logger.debug("no replacements found for the R2RML template!");
                                            null;
                                        }
                                        // Changed this from templateResult to templateResult(0) because I changed return type of replaceTemplateGroups
                                        // Side effects not guaranteed!
                                        templateResult(0);
                                    }
                                    case Constants.MorphTermMapType.ColumnTermMap | Constants.MorphTermMapType.ReferenceTermMap => {
                                        val rsObjectVarName = rs.getObject(varName).toString()
                                        if (rsObjectVarName == null)
                                            null
                                        else
                                            rsObjectVarName.toString();
                                    }
                                    case Constants.MorphTermMapType.ConstantTermMap => {
                                        termMap.getConstantValue();
                                    }
                                    case _ => {
                                        logger.error("Unsupported term map type!");
                                        null;
                                    }
                                }
                            } else
                                null;
                        }

                        val termMapType = termMap.inferTermType;
                        val resultAuxString = {
                            if (resultAux != null) {
                                if (termMapType != null) {
                                    if (termMapType.equals(Constants.R2RML_IRI_URI))
                                        GeneralUtility.encodeURI(resultAux, properties.mapURIEncodingChars, properties.uriTransformationOperation);
                                    else if (termMapType.equals(Constants.R2RML_LITERAL_URI))
                                        GeneralUtility.encodeLiteral(resultAux);
                                    else
                                        resultAux
                                } else {
                                    resultAux
                                }
                            } else {
                                null
                            }
                        }
                        new TermMapResult(resultAuxString, termMapType, termMap.datatype);
                    }
                } else {
                    null
                }
            } catch {
                case e: Exception => {
                    logger.debug("Error occured while translating result set : " + e.getMessage());
                    null;
                }
            }
        }
        result;
    }

    /**
     * Retrieve the column names that start like the SPARQL variable name => the columns from the result set,
     * that correspond to that SPARQL variable
     */
    private def getElementsStartWith(columnNames: List[String], varName: String): List[String] = {
        val result = columnNames.flatMap(columnName => {
            if (columnName.startsWith(varName))
                Some(columnName);
            else
                None;
        })
        result;
    }

    /**
     * Retrieve the value of the column corresponding to the SPARQL variable name from the result set
     */
    private def getMappedMappingByVarName(varName: String, rs: MorphBaseResultSet) = {
        try {
            val mappingHashCode = rs.getInt(Constants.PREFIX_MAPPING_ID + varName);

            //IN CASE OF UNION, A VARIABLE MAY BE MAPPED TO MULTIPLE MAPPINGS
            if (mappingHashCode == null)
                this.prSQLGenerator.getMappedMapping(varName.hashCode())
            else
                this.prSQLGenerator.getMappedMapping(mappingHashCode)
        } catch {
            case e: Exception => {
                null
            }
        }
    }
}
