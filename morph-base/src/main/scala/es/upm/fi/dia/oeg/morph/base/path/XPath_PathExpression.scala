package es.upm.fi.dia.oeg.morph.base.path

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class XPath_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    override def toString: String = {
        "XPath: " + pathExpression 
    }
}

object XPath_PathExpression {

    /**
     * Make an instance from a path construct expression like Column(expr) or JSONPath(expr)
     */
    def parse(pathConstructExpr: String): XPath_PathExpression = {
        
        // Remove the path constructor name "XPath(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_XPATH.length + 1, pathConstructExpr.length - 1)

        new XPath_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }
}