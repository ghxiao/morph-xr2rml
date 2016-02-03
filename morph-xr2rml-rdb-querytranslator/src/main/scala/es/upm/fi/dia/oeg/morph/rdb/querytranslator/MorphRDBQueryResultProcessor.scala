package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.io.Writer
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.querytranslator.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphXMLQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap

class MorphRDBQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
    xmlOutputStream: Writer,
    dataSourceReader: MorphBaseDataSourceReader,
    queryTranslator: IQueryTranslator)

        extends MorphXMLQueryResultProcessor(
            mappingDocument: R2RMLMappingDocument,
            properties: MorphProperties,
            xmlOutputStream: Writer) {

    override val logger = Logger.getLogger(this.getClass().getName());

    private var mapTemplateMatcher: Map[String, Matcher] = Map.empty;

    private var mapTemplateAttributes: Map[String, List[String]] = Map.empty;

    var projectionGenerator = queryTranslator.asInstanceOf[MorphRDBQueryTranslator].getPRSQLGen

    /**
     * Execute the query and translate the results from the database into triples.<br>
     * In the RDB case the UnionOfGenericQueries should contain only one element, since
     * the UNION is supported in SQL.<br>
     */
    def translateResult(mapSparqlSql: Map[Query, AbstractQuery]) {
        val start = System.currentTimeMillis();
        var i = 0;
        mapSparqlSql.foreach(mapElement => {
            val sparqlQuery = mapElement._1
            // In the RDB case there should be only a query, and one query in it
            val iQuery = mapElement._2.head.concreteQuery.asInstanceOf[ISqlQuery]

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
    override def process(sparqlQuery: Query, resultSet: MorphBaseResultSet) = {
        var i = 0;
        while (resultSet.next()) {
            val resultElement = xmlDoc.createElement("result");
            resultsElement.appendChild(resultElement);

            for (varName <- sparqlQuery.getResultVars()) {
                val translatedColVal = this.translateResultSet(varName, resultSet);
                if (translatedColVal != null) {
                    val lexicalValue = transformToLexical(translatedColVal.translatedValue, translatedColVal.xsdDatatype)
                    if (lexicalValue != null) {
                        val bindingElt = xmlDoc.createElement("binding");
                        bindingElt.setAttribute("name", varName);
                        resultElement.appendChild(bindingElt);
                        val termType = translatedColVal.termType;
                        if (termType != null) {
                            val termTypeEltName = {
                                if (termType.equalsIgnoreCase(Constants.R2RML_IRI_URI)) "uri";
                                else if (termType.equalsIgnoreCase(Constants.R2RML_LITERAL_URI)) "literal";
                                else null // what if this happens? Is it the case of a blank node?
                            }
                            if (termTypeEltName != null) {
                                val termTypeElt = xmlDoc.createElement(termTypeEltName);
                                bindingElt.appendChild(termTypeElt);
                                termTypeElt.setTextContent(lexicalValue);
                            }
                        } else
                            bindingElt.setTextContent(lexicalValue);
                    }
                }
            }
            i = i + 1;
        }
        logger.info(i + " variable result mappings retrieved");
    }

    private def translateResultSet(varName: String, rs: MorphBaseResultSet): TermMapResult = {
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
                                                if (columnNames == null || columnNames.isEmpty) {
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

                                        val templateResult = if (replacements.size > 0) {
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
                this.projectionGenerator.getMappedMapping(varName.hashCode())
            else
                this.projectionGenerator.getMappedMapping(mappingHashCode)
        } catch {
            case e: Exception => {
                null
            }
        }
    }

}