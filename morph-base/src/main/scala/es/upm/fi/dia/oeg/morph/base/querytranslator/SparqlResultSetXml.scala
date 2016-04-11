package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.XMLUtility
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory
import java.io.OutputStreamWriter
import java.io.PrintWriter
import org.w3c.dom.Document
import java.io.File

class SparqlResultSetXml(factory: IMorphFactory, xmlDoc: Document, sparqlQuery: Query) {

    def getDoc = { xmlDoc }

    /**
     * Save the XML document to a file
     */
    def save(output: File) = {
        val writer = new PrintWriter(output, "UTF-8")
        XMLUtility.saveXMLDocument(xmlDoc, writer)
    }

    override def toString: String = {
        xmlDoc.toString
    }
}

object SparqlResultSetXml {

    /**
     * Constructor that initializes the XML tree for the SPARQL result set with one variable node
     * for each projected variable in the SPARQL query
     * <pre>
     * &lt;sparql&gt;
     *   &lt;head&gt;
     *     &lt;/variable name="var1&gt;
     *     &lt;/variable name="var2&gt;
     *     ...
     *   &lt;/head&gt;
     * &lt;/sparql&gt;
     * </pre>
     */
    def apply(factory: IMorphFactory, sparqlQuery: Query): SparqlResultSetXml = {
        val xmlDoc = XMLUtility.createNewXMLDocument()

        //create root element
        val rootElement = xmlDoc.createElement("sparql");
        xmlDoc.appendChild(rootElement);

        //create head element
        val headElement = xmlDoc.createElement("head");
        rootElement.appendChild(headElement);
        val varNames = sparqlQuery.getResultVars()
        for (varName <- varNames) {
            val variableElement = xmlDoc.createElement("variable")
            variableElement.setAttribute("name", varName)
            headElement.appendChild(variableElement)
        }
        new SparqlResultSetXml(factory, xmlDoc, sparqlQuery)
    }
}
