package es.upm.fi.dia.oeg.morph.base.path

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import scala.util.matching.Regex.Match
import com.hp.hpl.jena.sparql.pfunction.library.str
import scala.collection.mutable.Queue

/**
 * A mixed syntax path consists of a sequence of path constructors each containing a path expression, example:
 * Column(NAME)/JSONPath($.*) : this expression retrieves the value of column NAME in a row-based database,
 * then apply the JSONPath expression to the value.
 *
 * A instance of MixedSyntaxPath holds a list of PathExpression instances, each corresponding to a part
 * of the path. A simple expression with no path constructor, like a simple column name or an XPath expression
 * are considered as a mixed syntax path with only one implicit path constructor.
 * The implicit path constructor is figured out from the reference formulation of the logical source.
 */
class MixedSyntaxPath(
        /** Reference value as provided by the mapping file */
        val rawValue: String,

        /** Reference formulation from the triples map logical source */
        val refFormulation: String,

        /** List of paths that compose the mixed syntax path */
        val paths: List[PathExpression]) {

    val logger = Logger.getLogger(this.getClass().getName())

    /**
     * This method <b>only</b> applies in the context of a row-based database, that is which a	mixed-syntax path
     * MUST start with a "Column(...)" path constructor.
     * It returns the column name referenced in the first path constructor if this is a "Column()",
     * otherwise None is returned.
     */
    def getReferencedColumn(): Option[String] = {
        if (refFormulation == xR2RML_Constants.xR2RML_COLUMN_URI) {
            val path = paths.head
            // In a column-based database, the first expression MUST be a Column() expression
            if (path.isInstanceOf[Column_PathExpression]) {
                Option(path.pathExpression)
            } else {
                logger.error("Error: the first path expression applied to a row-based database MUST be a Column() expression. Ignoring.")
                None
            }
        } else {
            logger.error("Error: there is no column reference in a non row-based database. Ignoring.")
            None
        }
    }
}

object MixedSyntaxPath {

    val logger = Logger.getLogger(this.getClass().getName())

    /**
     *  Parse the mixed syntax path value read from the mapping (properties xrr:reference or rr:template)
     *  and create corresponding PathExpression instances.
     *
     *  @param rawValue string value representing the path. This may be a mixed syntax path or
     *  a simple expression such as a column name or a JSONPath expression. If this is a simple expression,
     *  the reference formulation is used to know what type of expression this is
     *  @param refForm the reference formulation of the logical source of the current triples map
     *  @return an instance of MixedSyntaxPath
     */
    def apply(rawValue: String, refFormulation: String): MixedSyntaxPath = {

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

        logger.trace("Created MixedSyntaxPath, PathExpression's: " + result)
        new MixedSyntaxPath(rawValue, refFormulation, result)
    }

    /**
     * Unescape the chars escaped in a path construct expression, name '/', '(', ')', '{' and '}'
     */
    def unescapeChars(expr: String): String = {
        expr.replaceAll("""\\/""", "/").replaceAll("""\\\(""", "(").replaceAll("""\\\)""", ")").replaceAll("""\\\}""", "}").replaceAll("""\\\{""", "{")
    }

    def main(args: Array[String]) = {
        // Following characters must be escaped: /() => (\/) (\() (\)) (\{) (\})
        // Following characters must not be escaped: !#%&,-./:;<=>?@_`|~[]"'*+^$

        val pathConstructors = "(Column|XPath|JSONPath|CSV|TSV)"
        val pathChars = """([\p{Alnum}\p{Space}!#%&,-.:;<=>?(\\@)_`\|~\[\]\"\'\*\+\^\$]|(\\/)|(\\\()|(\\\)|(\\\{)|(\\\})))+"""
        val pathRegex = "(" + pathConstructors + """\(""" + pathChars + """\))""";

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"

        //println("Parse XPath: " + pathRegex.r.findAllMatchIn(xPath).toList)
        //println("Parse JSONPath: " + pathRegex.r.findAllMatchIn(jsonPath).toList)

        //println("Parse mixed syntax path: " + pathRegex.r.findAllMatchIn(mixedPath).toList);
        //println("Parse mixed syntax path: " + xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX.findAllMatchIn(mixedPath).toList);
        //println("Parse XPath with XPath regex: " + xR2RML_Constants.xR2RML_PATH_XPATH_REGEX.findAllMatchIn(xPath).toList);
        //println("Parse expr without path constructor: " + pathRegex.r.findAllMatchIn("NAME").toList);

        var isOk = xPath match {
            case pathRegex.r(_*) => " is a mixed syntax path"
            case _ => " is not a mixed syntax path"
        }
        //println(xPath + isOk)

        isOk = "NAME" match {
            case pathRegex.r(_*) => " is a mixed syntax path"
            case _ => " is not a mixed syntax path"
        }
        //println("NAME" + isOk)

        // Construct MixedSyntaxPath and test getReferencedColumn
        val msp = MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI)
        //println("List of PathExpression instances: " + msp.paths)
        //println("Column reference: " + msp.getReferencedColumn)

        // ------------------------------------------------------------------------------------------

        //val tpl2 = "http://example.org/student/{id}"
        var tpl2 = "http://example.org/student/{ID}/{" + mixedPath + "}"

        val mixedSntxRegex = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX

        // Save all path expressions in the template string
        val mixedSntxPaths: Queue[String] = Queue(mixedSntxRegex.findAllMatchIn(tpl2).toList.map(item => item.toString): _*)
        println("mixedSntxPaths: " + mixedSntxPaths)

        // Replace each path expression with a place holder "xR2RML_replacer"
        tpl2 = mixedSntxRegex.replaceAllIn(tpl2, "xR2RML_replacer")
        println(tpl2)

        // Make a list of the R2RML template groups between '{' '}'
        val tplPattern = """\{"*\w+\s*[\s\w/]*\"*}"""
        val listPattern = tplPattern.r.findAllIn(tpl2).toList
        println(listPattern)

        // Restore the path expressions in each of the place holders
        val listReplaced = MixedSyntaxPath.replaceTplPlaceholder(listPattern, mixedSntxPaths)
        println("Liste finale: " + listReplaced)

        // Extract the column references of each template group between '{' and '}'
        val colRefs = listReplaced.map(group =>
            MixedSyntaxPath(group, xR2RML_Constants.xR2RML_COLUMN_URI).getReferencedColumn
        )
        println("Column references: " + colRefs)

    }

    def replaceStrRec(template: String, replacers: Queue[String]): String = {
        if (replacers == Nil || template == Nil || !template.contains("xR2RML_replacer")) {
            template
        } else {
            val hd = replacers.dequeue

            val idx = template.indexOf("xR2RML_replacer")
            val str = template.substring(0, idx) + hd.toString + template.substring(idx + "xR2RML_replacer".length)
            replaceStrRec(str, replacers)
        }
    }

    /**
     * Replace placeholder 'xR2RML_replacer' in each template of the list tplList by
     * its original value that is a mixed-syntax path.
     * This method is used to deal with templates including mixed syntax path.
     * Those are quite complicated to parse at once with a regular expression, thus
     * this method.
     */
    def replaceTplPlaceholder(tplList: List[String], replacers: Queue[String]): List[String] = {
        if (tplList == Nil)
            Nil
        else {
            val str = replaceStrRec(tplList.head.toString, replacers)
            str :: replaceTplPlaceholder(tplList.tail, replacers)
        }
    }

}