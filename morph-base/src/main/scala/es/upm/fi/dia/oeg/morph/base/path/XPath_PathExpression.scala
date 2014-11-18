package es.upm.fi.dia.oeg.morph.base.path

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import java.io.IOException
import javax.xml.xpath.XPathFactory
import java.io.StringReader
import javax.xml.xpath.XPathConstants
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.dom.DOMSource
import java.io.StringWriter
import javax.xml.transform.Transformer
import javax.xml.transform.TransformerFactory
import org.w3c.dom.Node
import javax.xml.xpath.XPathExpressionException
import org.apache.log4j.Logger

class XPath_PathExpression(pathExpression: String)
        extends PathExpression(pathExpression) {

    val logger = Logger.getLogger(this.getClass().getName())

    private val xPath = XPathFactory.newInstance().newXPath()

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
            val res = xPath.compile(pathExpression).evaluate(xmlDoc, XPathConstants.NODESET)
            val nodes = res.asInstanceOf[org.w3c.dom.NodeList]

            // Translate the resulting node set into a list of strings
            val result = for (i <- 0 to nodes.getLength - 1) yield XPath_PathExpression.getValue(nodes.item(i))
            result.toList

        } catch {
            case e: XPathExpressionException => {
                logger.error("Invalid XPath expression: " + pathExpression + ", or invalid XML input data: " + value)
                // Value the original value as is, that's the best we can do here
                List(value)
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
                writer.toString.replace("\n", "").replace("\r", "").replaceAll(">[ \t]+<", "><")
            } else
                node.getTextContent
        }
    }

    /** For debug purpose only */
    def main(args: Array[String]) = {

        val xmlStr = """
            <Employees>
            <Employee emplid="1111" type="admin">
        		<firstname>John</firstname>
        		<lastname>Watson</lastname>
        		<age>30</age>
        		<email>johnwatson@sh.com</email>
    		</Employee>            
            <Employee emplid="2222">
        		<firstname>Paul</firstname>
        		<email>boo@foo.com</email>
        		<age>40</age>
    		</Employee>"
            </Employees>"""

        // Result = 2 XML elements
        println(new XPath_PathExpression("""//Employee/age""").evaluate(xmlStr))
        // Result = values of attribute
        println(new XPath_PathExpression("""//Employee/@emplid""").evaluate(xmlStr))
        // Result = serialized XML subtree
        println(new XPath_PathExpression("""//Employee""").evaluate(xmlStr))
        // Result = nothing
        println(new XPath_PathExpression("""//foo""").evaluate(xmlStr))
        // Error in the XPath
        println(new XPath_PathExpression("""/dsff/[dfsf}/foo""").evaluate(xmlStr))
        // Error in the XML
        println(new XPath_PathExpression("""//foo""").evaluate(xmlStr + "<ggdf"))
    }
}