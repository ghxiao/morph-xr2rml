package es.upm.fi.dia.oeg.morph.base.path

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class CSV_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    override def toString: String = { "CSV: " + pathExpression }

    def evaluate(value: String): List[Object] = {
        throw new Exception("Unsupported operation evaluate")
    }

}

object CSV_PathExpression {

    /**
     * Make an instance from a path construct expression like CSV(expr)
     */
    def parse(pathConstructExpr: String): CSV_PathExpression = {

        // Remove the path constructor name "CSV(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_CSV.length + 1, pathConstructExpr.length - 1)

        new CSV_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }
}