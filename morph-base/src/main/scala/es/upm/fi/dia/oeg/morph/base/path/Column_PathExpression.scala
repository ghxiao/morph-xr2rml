package es.upm.fi.dia.oeg.morph.base.path

import es.upm.fi.dia.oeg.morph.base.Constants

class Column_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    override def toString: String = { "Column[" + pathExpression + "]" }

    def evaluate(value: String): List[Object] = {
        throw new Exception("Path constructor Column() only allowed as first path of a mixed syntax path")
    }
}

object Column_PathExpression {

    /**
     * Make an instance from a path construct expression like Column(expr)
     */
    def parse(pathConstructExpr: String): Column_PathExpression = {

        // Remove the path constructor name "Column(" and the final ")"
        var expr = pathConstructExpr.trim().substring(Constants.xR2RML_PATH_CONSTR_COLUMN.length + 1, pathConstructExpr.length - 1)

        new Column_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }
}