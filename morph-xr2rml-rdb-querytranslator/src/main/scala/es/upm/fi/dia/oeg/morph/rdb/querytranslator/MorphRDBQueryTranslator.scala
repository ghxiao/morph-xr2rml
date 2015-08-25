package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node

import Zql.ZConstant
import Zql.ZExp
import es.upm.fi.dia.oeg.morph.base.CollectionUtility
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseAlphaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.NameGenerator
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

class MorphRDBQueryTranslator(nameGenerator: NameGenerator, alphaGenerator: MorphBaseAlphaGenerator, betaGenerator: MorphBaseBetaGenerator, condSQLGenerator: MorphBaseCondSQLGenerator, prSQLGenerator: MorphBasePRSQLGenerator)
        extends MorphBaseQueryTranslator(nameGenerator: NameGenerator, alphaGenerator: MorphBaseAlphaGenerator, betaGenerator: MorphBaseBetaGenerator, condSQLGenerator: MorphBaseCondSQLGenerator, prSQLGenerator: MorphBasePRSQLGenerator) {

    override val logger = Logger.getLogger("MorphQueryTranslator");
    this.alphaGenerator.owner = this;
    this.betaGenerator.owner = this;

    var mapTemplateMatcher: Map[String, Matcher] = Map.empty;
    var mapTemplateAttributes: Map[String, List[String]] = Map.empty;

    override def transIRI(node: Node): List[ZExp] = {
        val cms = mapInferredTypes(node);
        val cm = cms.iterator().next().asInstanceOf[R2RMLTriplesMap];
        val mapColumnsValues = cm.subjectMap.getTemplateValues(node.getURI());
        val result: List[ZExp] = {
            if (mapColumnsValues == null || mapColumnsValues.size() == 0) {
                //do nothing
                Nil
            } else {
                val resultAux = mapColumnsValues.keySet.map(column => {
                    val value = mapColumnsValues(column);
                    val constant = new ZConstant(value, ZConstant.UNKNOWN);
                    constant;
                })
                resultAux.toList;
            }
        }
        result;
    }

    def getMappedMappingByVarName(varName: String, rs: MorphBaseResultSet) = {
        val mapValue = {
            try {
                val mappingHashCode = rs.getInt(Constants.PREFIX_MAPPING_ID + varName);

                //IN CASE OF UNION, A VARIABLE MAY MAPPED TO MULTIPLE MAPPINGS
                if (mappingHashCode == null) {
                    val varNameHashCode = varName.hashCode();
                    this.prSQLGenerator.getMappedMapping(varNameHashCode)
                } else {
                    this.prSQLGenerator.getMappedMapping(mappingHashCode)
                }
            } catch {
                case e: Exception => {
                    null
                }
            }
        }
        mapValue;
    }

    override def translateResultSet(varName: String, rs: MorphBaseResultSet): TermMapResult = {
        val result: TermMapResult = {
            try {
                if (rs != null) {
                    val rsColumnNames = rs.getColumnNames();
                    val columnNames = CollectionUtility.getElementsStartWith(rsColumnNames, varName + "_");

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
                                    val md = this.mappingDocument.asInstanceOf[R2RMLMappingDocument];
                                    val parentTriplesMap = md.getParentTriplesMap(mappedValueRefObjectMap);
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
                                val termMapType = termMap.termMapType;
                                termMap.termMapType match {
                                    case Constants.MorphTermMapType.TemplateTermMap => {
                                        val templateString = termMap.getTemplateString();
                                        if (this.mapTemplateMatcher.contains(templateString)) {
                                            val matcher = this.mapTemplateMatcher.get(templateString);
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
                                        if (rsObjectVarName == null) {
                                            null
                                        } else {
                                            rsObjectVarName.toString();
                                        }
                                    }
                                    case Constants.MorphTermMapType.ConstantTermMap => {
                                        termMap.getConstantValue();
                                    }
                                    case _ => {
                                        logger.error("Unsupported term map type!");
                                        null;
                                    }
                                }
                            } else {
                                null;
                            }
                        }

                        val termMapType = termMap.inferTermType;
                        val xsdDatatype = termMap.datatype;
                        val resultAuxString = {
                            if (resultAux != null) {
                                if (termMapType != null) {
                                    if (termMapType.equals(Constants.R2RML_IRI_URI)) {
                                        GeneralUtility.encodeURI(resultAux, properties.mapURIEncodingChars, properties.uriTransformationOperation);
                                    } else if (termMapType.equals(Constants.R2RML_LITERAL_URI)) {
                                        GeneralUtility.encodeLiteral(resultAux);
                                    } else {
                                        resultAux
                                    }
                                } else {
                                    resultAux
                                }
                            } else {
                                null
                            }
                        }
                        new TermMapResult(resultAuxString, termMapType, xsdDatatype);
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
}
