package es.upm.fi.dia.oeg.morph.base.path

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class JSONPath_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    override def toString: String = {
        "JSONPath: " + pathExpression 
    }
}

object JSONPath_PathExpression {

    /**
     * Make an instance from a path construct expression like JSONPath(expr)
     */
    def parse(pathConstructExpr: String): JSONPath_PathExpression = {
        
        // Remove the path constructor name "JSONPath(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_JSONPATH .length + 1, pathConstructExpr.length - 1)

        new JSONPath_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }
}