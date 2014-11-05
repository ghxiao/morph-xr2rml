package es.upm.fi.dia.oeg.morph.base.path

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class TSV_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    override def toString: String = {
        "TSV: " + pathExpression 
    }
}

object TSV_PathExpression {

    /**
     * Make an instance from a path construct expression like TSV(expr)
     */
    def parse(pathConstructExpr: String): TSV_PathExpression = {
        
        // Remove the path constructor name "TSV(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_TSV.length + 1, pathConstructExpr.length - 1)

        new TSV_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }
}