package es.upm.fi.dia.oeg.morph.base.path

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class MixedSyntaxPath(
        /** Reference value as provided by the mapping file */
        val rawValue: String,
        val refFormulation: String) {

    val logger = Logger.getLogger(this.getClass().getName())

    /** List of paths that compose the mixed syntax path */
    val paths: List[PathExpression] = this.parse(rawValue, refFormulation)

    /**
     *  Parse the mixed syntax path value read from the mapping (properties xrr:reference or rr:template)
     *  and create corresponding PathExpression instances.
     *
     *  @param rawValue string value representing the path. This may be a mixed syntax path or
     *  a simple expression such as a column name or a JSONPath expression. If this is a simple expression,
     *  the reference formulation is used to know what type of expression this is
     *  @param refForm the reference formulation of the logical source of the current triples map
     */
    def parse(rawValue: String, refForm: String): List[PathExpression] = {

        // Split the mixed syntax path into individual path construct expressions, like "Column(NAME)"  
        val rawPathList = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX.findAllMatchIn(rawValue).toList

        val result =
            if (rawPathList.isEmpty) {
                // The value is a simple path expression without any path construct
                val res = refFormulation match {
                    case xR2RML_Constants.xR2RML_COLUMN_URI => new Column_PathExpression(rawValue)
                    case xR2RML_Constants.QL_XPATH_URI => new XPath_PathExpression(rawValue)
                    case xR2RML_Constants.QL_JSONPATH_URI => new JSONPath_PathExpression(rawValue)
                    case _ => throw new Exception("Unknown reference formulation: " + refFormulation)
                }
                List(res)

            } else {
                // The value is a mixed syntax path. Each individual path (in the path constructor) is parsed 
                rawPathList.map(rawPath =>
                    rawPath match {
                        case xR2RML_Constants.xR2RML_PATH_COLUMN_REGEX(_*) => { Column_PathExpression.parse(rawPath.toString) }
                        case xR2RML_Constants.xR2RML_PATH_XPATH_REGEX(_*) => { XPath_PathExpression.parse(rawPath.toString) }
                        case xR2RML_Constants.xR2RML_PATH_JSONPATH_REGEX(_*) => { JSONPath_PathExpression.parse(rawPath.toString) }
                        case xR2RML_Constants.xR2RML_PATH_CSV_REGEX(_*) => { CSV_PathExpression.parse(rawPath.toString) }
                        case xR2RML_Constants.xR2RML_PATH_TSV_REGEX(_*) => { TSV_PathExpression.parse(rawPath.toString) }
                        case _ => throw new Exception("Unknown type of path: " + rawPath)
                    }
                )
            }

        logger.trace("List of raw path expressions: " + rawPathList)
        logger.trace("List of PathExpression instances: " + result)
        result
    }
}

object MixedSyntaxPath {

    /**
     * Un-escape the chars escaped in a path construct expression, name '/', '(' and ')'
     */
    def unescapeChars(expr: String): String = {
        expr.replaceAll("""\\/""", "/").replaceAll("""\\\(""", "(").replaceAll("""\\\)""", ")")

    }

    def main(args: Array[String]) = {
        // Following characters must be escaped: /() => (\/)(\()(\))
        // Following characters must not be escaped: !#%&,-./:;<=>?@_`|~[]"'*+^${}

        val pathConstructors = "(Column|XPath|JSONPath|CSV|TSV)"
        val pathChars = """([\p{Alnum}\p{Space}!#%&,-.:;<=>?(\\@)_`\|~\[\]\"\'\*\+\^\$\{\}]|(\\/)|(\\\()|(\\\)))+"""
        val pathRegex = "(" + pathConstructors + """\(""" + pathChars + """\))""";

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"

        System.out.println("Parse XPath: " + pathRegex.r.findAllMatchIn(xPath).toList)
        System.out.println("Parse JSONPath: " + pathRegex.r.findAllMatchIn(jsonPath).toList)

        System.out.println("Parse mixed syntax path: " + pathRegex.r.findAllMatchIn(mixedPath).toList);
        System.out.println("Parse mixed syntax path: " + xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX.findAllMatchIn(mixedPath).toList);
        System.out.println("Parse XPath with XPath regex: " + xR2RML_Constants.xR2RML_PATH_XPATH_REGEX.findAllMatchIn(xPath).toList);
        System.out.println("Parse expr without path constructor: " + pathRegex.r.findAllMatchIn("NAME").toList);

        var isOk = xPath match {
            case pathRegex.r(_*) => " is a mixed syntax path"
            case _ => " is not a mixed syntax path"
        }
        //System.out.println(xPath + isOk)

        isOk = "NAME" match {
            case pathRegex.r(_*) => " is a mixed syntax path"
            case _ => " is not a mixed syntax path"
        }
        //System.out.println("NAME" + isOk)

        val msp = new MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI)
        System.out.println("List of PathExpression instances: " + msp.paths)
    }
}