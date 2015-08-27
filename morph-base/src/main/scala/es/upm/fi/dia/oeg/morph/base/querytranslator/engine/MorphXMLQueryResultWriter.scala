package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import java.io.Writer
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.log4j.Logger
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.materializer.XMLUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

class MorphXMLQueryResultWriter(queryTranslator: IQueryTranslator, xmlOutputStream: Writer)
        extends MorphBaseQueryResultWriter(queryTranslator, xmlOutputStream) {
    this.outputStream = xmlOutputStream;

    val logger = Logger.getLogger(this.getClass().getName());

    if (queryTranslator == null) {
        throw new MorphException("Query Translator is not set yet!");
    }

    val xmlDoc = XMLUtility.createNewXMLDocument();
    val resultsElement = xmlDoc.createElement("results");

    override def initialize() = {}

    private def transformToLexical(originalValue: String, pDatatype: Option[String]): String = {
        if (pDatatype.isDefined && originalValue != null) {
            val datatype = pDatatype.get;
            val xsdDateTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
            val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();

            if (datatype.equals(xsdDateTimeURI)) {
                originalValue.trim().replaceAll(" ", "T");
            } else if (datatype.equals(xsdBooleanURI)) {
                if (originalValue.equalsIgnoreCase("T") || originalValue.equalsIgnoreCase("True")) {
                    "true";
                } else if (originalValue.equalsIgnoreCase("F") || originalValue.equalsIgnoreCase("False")) {
                    "false";
                } else {
                    "false";
                }
            } else {
                originalValue
            }
        } else {
            originalValue
        }
    }

    def preProcess() = {

        //create root element
        val rootElement = xmlDoc.createElement("sparql");
        xmlDoc.appendChild(rootElement);

        //create head element
        val headElement = xmlDoc.createElement("head");
        rootElement.appendChild(headElement);
        val sparqlQuery = this.sparqlQuery;
        val varNames = sparqlQuery.getResultVars();
        for (varName <- varNames) {
            val variableElement = xmlDoc.createElement("variable");
            variableElement.setAttribute("name", varName);
            headElement.appendChild(variableElement);
        }

        //create results element
        rootElement.appendChild(resultsElement);
    }

    def process() = {
        val queryTranslator = this.queryTranslator;
        val sparqlQuery = this.sparqlQuery;
        val varNames = sparqlQuery.getResultVars();

        var i = 0;
        val rs = this.resultSet;
        while (rs.next()) {
            val resultElement = xmlDoc.createElement("result");
            resultsElement.appendChild(resultElement);

            for (varName <- varNames) {
                val translatedColumnValue = queryTranslator.translateResultSet(varName, rs);
                if (translatedColumnValue != null) {
                    val translatedDBValue = translatedColumnValue.translatedValue;
                    val xsdDataType = translatedColumnValue.xsdDatatype;
                    val lexicalValue = transformToLexical(translatedDBValue, xsdDataType)
                    if (lexicalValue != null) {
                        val bindingElement = xmlDoc.createElement("binding");
                        bindingElement.setAttribute("name", varName);
                        resultElement.appendChild(bindingElement);
                        val termType = translatedColumnValue.termType;
                        if (termType != null) {
                            val termTypeElementName = {
                                if (termType.equalsIgnoreCase(Constants.R2RML_IRI_URI)) {
                                    "uri";
                                } else if (termType.equalsIgnoreCase(Constants.R2RML_LITERAL_URI)) {
                                    "literal";
                                } else {
                                    null
                                }
                            }
                            val termTypeElement = xmlDoc.createElement(termTypeElementName);
                            bindingElement.appendChild(termTypeElement);
                            termTypeElement.setTextContent(lexicalValue);
                        } else {
                            bindingElement.setTextContent(lexicalValue);
                        }
                    }
                }
            }
            i = i + 1;
        }
        val status = i + " instance(s) retrieved ";
        logger.debug("Results: " + this.resultSet)
        logger.info(status);

    }

    def postProcess() = {
        logger.info("Writing query result to " + outputStream);
        XMLUtility.saveXMLDocument(xmlDoc, outputStream);
    }

    override def getOutput() = {
        this.xmlDoc;
    }

}