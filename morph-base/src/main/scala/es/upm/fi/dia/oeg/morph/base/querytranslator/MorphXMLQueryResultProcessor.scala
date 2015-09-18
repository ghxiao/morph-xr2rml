package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.XMLUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

abstract class MorphXMLQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
    xmlOutputStream: Writer,
    dataSourceReader: MorphBaseDataSourceReader)

        extends MorphBaseQueryResultProcessor(
            mappingDocument,
            properties,
            xmlOutputStream,
            dataSourceReader) {

    this.outputStream = xmlOutputStream;

    override val logger = Logger.getLogger(this.getClass().getName());

    val xmlDoc = XMLUtility.createNewXMLDocument();
    val resultsElement = xmlDoc.createElement("results");

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
    override def preProcess(sparqlQuery: Query) = {
        //create root element
        val rootElement = xmlDoc.createElement("sparql");
        xmlDoc.appendChild(rootElement);

        //create head element
        val headElement = xmlDoc.createElement("head");
        rootElement.appendChild(headElement);
        val varNames = sparqlQuery.getResultVars();
        for (varName <- varNames) {
            val variableElement = xmlDoc.createElement("variable");
            variableElement.setAttribute("name", varName);
            headElement.appendChild(variableElement);
        }

        //create results element
        rootElement.appendChild(resultsElement);
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

    /**
     * Save the XML document to a file
     */
    override def postProcess() = {
        if (logger.isDebugEnabled()) logger.debug("Writing query result document:\n" + XMLUtility.printXMLDocument(xmlDoc, true, false))
        XMLUtility.saveXMLDocument(xmlDoc, outputStream);
    }

    override def getOutput() = {
        this.xmlDoc;
    }

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
            } else
                originalValue
        } else
            originalValue
    }
}