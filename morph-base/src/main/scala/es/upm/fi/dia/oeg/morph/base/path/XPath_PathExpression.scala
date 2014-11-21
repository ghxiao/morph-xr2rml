package es.upm.fi.dia.oeg.morph.base.path

import java.io.StringReader
import java.io.StringWriter

import org.apache.log4j.Logger
import org.w3c.dom.Node

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import javax.xml.transform.Transformer
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathExpression
import javax.xml.xpath.XPathExpressionException
import javax.xml.xpath.XPathFactory

class XPath_PathExpression(pathExpression: String)
        extends PathExpression(pathExpression) {

    private val logger = Logger.getLogger(this.getClass().getName())

    private val xPath = XPathFactory.newInstance().newXPath()

    private val compiledXPath: XPathExpression = try {
        xPath.compile(pathExpression)
    } catch {
        case e: XPathExpressionException => {
            logger.error("Invalid XPath expression: " + pathExpression)
            xPath.compile("/")
        }
    }

    override def toString: String = { "XPath: " + pathExpression }

    /**
     * Evaluate an XML value against the XPath expression represented by this, and return a list of values.
     * If a value selected by the XPath is not a leaf element (has children elements), the method returns
     * a serialization of the XML value.
     * If the XPath expression or the XML value is invalid, an error is logged and the method returns the value as is.
     *
     * @value the XML value to parse
     * @return list of values resulting from the evaluation
     */
    def evaluate(value: String): List[Object] = {
        try {
            // Run the XPath evaluation 
            val xmlDoc = new org.xml.sax.InputSource(new StringReader(value))
            val res = compiledXPath.evaluate(xmlDoc, XPathConstants.NODESET)
            val nodes = res.asInstanceOf[org.w3c.dom.NodeList]

            // Translate the resulting node set into a list of strings
            val result = for (i <- 0 to nodes.getLength - 1) yield XPath_PathExpression.getValue(nodes.item(i))
            result.toList

        } catch {
            case e: XPathExpressionException => {
                logger.error("XPath expression failed, invalid XML input data: " + value + ". Returning the input data as is.")
                // Value the original value as is, that's the best we can do here
                List(XPath_PathExpression.onOneLine(value))
            }
            case e: Exception => {
                val errMsg = "Unexpected error when evaluating XPath expression: " + pathExpression + ", with XML data: " + value
                logger.error(errMsg + ". Exception: " + e.getMessage() + "\n" + e.getStackTraceString)
                throw new Exception(errMsg)
            }
        }
    }
}

object XPath_PathExpression {

    private val xmlTransformer: Transformer = TransformerFactory.newInstance().newTransformer()

    /**
     * Make an instance from a path construct expression like Column(expr) or JSONPath(expr)
     */
    def parse(pathConstructExpr: String): XPath_PathExpression = {

        // Remove the path constructor name "XPath(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_XPATH.length + 1, pathConstructExpr.length - 1)

        new XPath_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }

    /**
     * Print the value of an DOM node: if this is a leaf XML element with a text value or attribute) then
     * return its text value, if this is an XML element with children XML elements then return
     * a serialization of this sub-tree.
     */
    private def getValue(node: Node) = {

        if (node.getFirstChild() == null) {
            node.getTextContent
        } else {
            // Check if the node has children
            val children = node.getChildNodes()
            var hasElementChildren = false
            for (j <- 0 to children.getLength - 1)
                if (children.item(j).getNodeType() == org.w3c.dom.Node.ELEMENT_NODE)
                    hasElementChildren = true

            // If the node has children, and at least one of them is an XML element, then
            // the node is the root of an XML sub-tree, we simply serialize it
            if (hasElementChildren) {
                val writer = new StringWriter()
                xmlTransformer.transform(new DOMSource(node), new StreamResult(writer));
                onOneLine(writer.toString)
            } else
                node.getTextContent
        }
    }

    def onOneLine(str: String): String = {
        str.trim.replace("\n", "").replace("\r", "").replaceAll(">[ \t]+<", "><")
    }

}