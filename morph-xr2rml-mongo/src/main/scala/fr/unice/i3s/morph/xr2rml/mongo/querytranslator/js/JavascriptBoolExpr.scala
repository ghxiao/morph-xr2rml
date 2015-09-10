package fr.unice.i3s.morph.xr2rml.mongo.querytranslator.js

import org.mozilla.javascript.Token
import org.mozilla.javascript.ast.AstNode
import scala.collection.mutable.Queue
import es.upm.fi.dia.oeg.morph.base.Constants

/**
 * This class is an abstract representation of JavaScript boolean expressions, based on the Rhino AstNode class.
 * It is used during the translation of JSONPath expressions containing JavaScript parts, into a MongoDB query.
 * Examples of such JSONPaths: $.p[?(@.q == @.r)].r, $[?(@.q && @.r.length >= 10)].s
 * 
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
class JavascriptBoolExpr(val astNode: AstNode) {

    val id: Int = astNode.hashCode

    var parentAstNode: AstNode = astNode.getParent

    val children: Queue[JavascriptBoolExpr] = Queue[JavascriptBoolExpr]()

    def getType = astNode.getType

    def astNodeToString = { Token.typeToName(astNode.getType) + ": " + astNode.toSource }

    /**
     * Check if the type of astNode is in the given list of types
     *
     * @param typeList list of types from org.mozilla.javascript.Token
     * @return true if astNode.getType is in typeList
     */
    def hasTypeIn(typeList: List[Int]): Boolean = {
        typeList.find(_ == this.astNode.getType).isDefined
    }

    /**
     * Debug-only method, quite verbose
     */
    override def toString: String = {
        var retrait = ""
        for (i <- 0 to astNode.depth())
            retrait = retrait + "  "

        val sep = Constants.SEPARATOR
        var str = retrait + this.astNodeToString + sep
        for (child <- children)
            str += retrait + child.toString + sep
        str
    }
}