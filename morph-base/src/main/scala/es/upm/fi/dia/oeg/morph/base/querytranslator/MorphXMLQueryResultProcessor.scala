package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.io.Writer

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.XMLUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

abstract class MorphXMLQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
    xmlOutputStream: Writer)

        extends MorphBaseQueryResultProcessor(
            mappingDocument,
            properties,
            xmlOutputStream) {

    this.outputStream = xmlOutputStream;

    val logger = Logger.getLogger(this.getClass().getName());

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
     * Save the XML document to a file
     */
    override def postProcess() = {
        if (logger.isDebugEnabled()) logger.debug("Writing query result document:\n" + XMLUtility.printXMLDocument(xmlDoc, true, false))
        XMLUtility.saveXMLDocument(xmlDoc, outputStream);
    }

    override def getOutput() = {
        this.xmlDoc;
    }

    def transformToLexical(originalValue: String, pDatatype: Option[String]): String = {
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