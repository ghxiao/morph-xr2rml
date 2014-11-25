package es.upm.fi.dia.oeg.morph.base.path

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.immutable.List

import org.apache.log4j.Logger

import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.InvalidPathException
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.ParseContext
import com.jayway.jsonpath.ReadContext

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class JSONPath_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    /** JSON parse configuration */
    val jsonConf: Configuration = com.jayway.jsonpath.Configuration.defaultConfiguration()
        .addOptions(com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST)
        .addOptions(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS);

    /** JSON parse context */
    val jsonParseCtx: ParseContext = JsonPath.using(jsonConf)

    override def toString: String = { "JSONPath: " + pathExpression }

    val logger = Logger.getLogger(this.getClass().getName())

    /**
     * Evaluate a JSON value against the JSONPath expression represented by this, and return a list of values.
     * If a value selected by the JSONPath is not a literal (dictionary or array), the method returns
     * a serialization of the value, like "["a","b"] for an array.
     * If the JSONPath expression or JSON value is invalid, an error is logged and the method returns the value as is.
     *
     * @value the JSON value to parse
     * @return list of values resulting from the evaluation
     */
    def evaluate(jsonValue: String): List[Object] = {
        try {
            val ctx: ReadContext = jsonParseCtx.parse(jsonValue);
            var result: java.util.List[Object] = ctx.read(pathExpression)

            result.toList.map(
                item => {
                    // Jayway JSONPath evaluation returns either a net.minidev.json.JSONArray,
                    // a java.util.LinkedHashMap for a JSON dictionary, 
                    // or any other simple type for literals: String, integer etc.
                    item match {
                        case arr: net.minidev.json.JSONArray => { arr.toJSONString }
                        case dic: java.util.LinkedHashMap[String, Object] => { net.minidev.json.JSONObject.toJSONString(dic) }
                        case _ => { item }
                    }
                }
            )
        } catch {
            case e: InvalidPathException => {
                logger.error("Invalid JSONPath expression: " + pathExpression + ". Exception: " + e.getMessage())
                List(jsonValue)
            }
        }
    }
}

object JSONPath_PathExpression {
    /**
     * Make an instance from a path construct expression like JSONPath(expr)
     */
    def parse(pathConstructExpr: String): JSONPath_PathExpression = {

        // Remove the path constructor name "JSONPath(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_JSONPATH.length + 1, pathConstructExpr.length - 1)

        new JSONPath_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }

    /**
     * Make an instance from a raw JSONPath expression (with no path constructor)
     */
    def parseRaw(pathConstructExpr: String): JSONPath_PathExpression = {
        new JSONPath_PathExpression(MixedSyntaxPath.unescapeChars(pathConstructExpr.trim()))
    }
}