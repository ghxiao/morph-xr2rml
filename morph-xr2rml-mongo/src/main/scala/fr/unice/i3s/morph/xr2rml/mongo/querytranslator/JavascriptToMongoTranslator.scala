package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.apache.log4j.Logger
import org.mozilla.javascript.Token

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCompare
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotSupported
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeWhere
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.js.JavascriptBoolExpr
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.js.JavascriptBoolExprFactory

/**
 * In the process of translating a JSONPath expression and a top-level condition into a MongoDB query,
 * it can be needed to translate JavaScript parts into equivalent MongoDB query operators.
 * This happens when a JSONPath expression contains a JavaScript boolean expression like:
 * 		$.p[?(@.p == 10)]
 * or a numerical JavaScript expression like:
 * 		$.p[(@.length - 1)]
 *
 * To understand this code, you must have read and understood the algorithm first. Ask the author for the reference.
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
object JavascriptToMongoTranslator {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Translate a JavaScript boolean expression (coming from a JSONPath expression)
     * into a MongoDB query object.
     * The JavaScript expression is first parsed by the Mozilla Rhino JavaScript engine,
     * then it is translated into an abstract MongoDB query by means of a hierarchy of
     * MongoQueryNode instances.
     *
     * @return a MongoQueryNode instance representing the top-level object, or None if any abnormal event occurs
     */
    def transJS(jsExpr: String): Option[MongoQueryNode] = {

        val absJs = JavascriptBoolExprFactory.create(jsExpr)
        if (absJs.isEmpty)
            return None

        if (absJs.get.astNode.getType() != Token.SCRIPT) {
            logger.error("JS expression [" + jsExpr + "] was parsed successfully but the result does not start with a SCRIPT top-level object. Skipping process.")
            return None
        }
        if (absJs.get.children.length != 1) {
            logger.error("JS expression [" + jsExpr + "] was parsed successfully but there is more than 1 child to the SCRIPT top-level object. Skipping process.")
            return None
        }
        Some(transJS(absJs.get.children(0)))
    }

    /**
     * Translate a JavaScript boolean expression into a MongoDB query object.
     *
     * @return a MongoQueryNode instance representing the top-level object. Should not be null.
     */
    private def transJS(jsExpr: JavascriptBoolExpr): MongoQueryNode = {

        val PropOrElem = List(Token.GETPROP, Token.GETELEM)
        val Values = List(Token.STRING, Token.NUMBER, Token.NULL, Token.FALSE, Token.TRUE)

        // ------ Rule j0: expr1 && expr2
        //	      transJS(<JS_expr1> && <JS_expr2>) :- AND(transJS(<JS_expr1>), transJS(<JS_expr2>))
        if (jsExpr.astNode.getType == Token.AND) {
            if (logger.isDebugEnabled()) logger.debug("JS expression [" + jsExpr.astNode.toSource + "] matches rule j0")
            val andMembers = jsExpr.children.toList.map(node => JavascriptToMongoTranslator.transJS(node))
            return new MongoQueryNodeAnd(andMembers)
        }

        // ------ Rule j1: expr1 || expr2
        // 	 	  transJS(<JS_expr1> || <JS_expr2>) :- OR(transJS(<JS_expr1>), transJS(<JS_expr2>))
        if (jsExpr.astNode.getType == Token.OR) {
            if (logger.isDebugEnabled()) logger.debug("JS expression [" + jsExpr.astNode.toSource + "] matches rule j1")
            val orMembers = jsExpr.children.toList.map(node => JavascriptToMongoTranslator.transJS(node))
            return new MongoQueryNodeOr(orMembers)
        }

        // ------ Rule j2: this<JS_expr1> <op> this<JS_expr2>, with <op> in {==, ===, !=, !== <=, <, >=, >}
        //		  transJS(this<JS_expr1> <op> this<JS_expr2>) :- AND(EXISTS(<JS_expr1>), EXISTS(<JS_expr2>), WHERE("this<JS_expr1> <op> this<JS_expr2>"))
        if (jsExpr hasTypeIn List(Token.EQ, Token.SHEQ, Token.NE, Token.SHNE, Token.LE, Token.LT, Token.GE, Token.GT)) {
            val child0 = jsExpr.children(0)
            val child1 = jsExpr.children(1)

            if ((child0 hasTypeIn PropOrElem) && (child1 hasTypeIn PropOrElem)) {
                val child0Src = child0.astNode.toSource
                val child1Src = child1.astNode.toSource
                if (child0Src.startsWith("this") && child1Src.startsWith("this")) {
                    logger.warn("Unupported JS expression [" + jsExpr.astNode.toSource + "], rule j2: pattern \"this<JS_expr> <op> this<JS_expr>\".")
                    return new MongoQueryNodeNotSupported(jsExpr.astNodeToString)
                    /* return new MongoQueryNodeAnd(List(
                        new MongoQueryNodeExists(child0Src.substring(4)),
                        new MongoQueryNodeExists(child1Src.substring(4)),
                        new MongoQueryNodeWhere(jsExpr.astNode.toSource)
                    )) */
                }
            }
        }

        // ------ Rule j3: this<JSpath>
        //		  transJS(this<JSpath>) :- EXISTS(<JSpath>)
        if (jsExpr hasTypeIn PropOrElem) {
            val source = jsExpr.astNode.toSource
            if (source.startsWith("this")) {
                if (logger.isDebugEnabled()) logger.debug("JS expression [" + jsExpr.astNode.toSource + "] matches rule j3")
                return new MongoQueryNodeField(source.substring(4), new MongoQueryNodeExists) // skip the "this"
            }
        }

        // ------ Rule j4: !this<JSpath>
        //		  transJS(!this<JSpath>) :- NOT_EXISTS(<JSpath>)
        if (jsExpr.getType == Token.NOT) {
            if (jsExpr.children(0) hasTypeIn PropOrElem) {
                val source = jsExpr.astNode.toSource
                if (source.startsWith("!this")) {
                    if (logger.isDebugEnabled()) logger.debug("JS expression [" + jsExpr.astNode.toSource + "] matches rule j4")
                    return new MongoQueryNodeField(source.substring(5), new MongoQueryNodeNotExists) // skip the "!this"
                }
            }
        }

        // ------ Rule j5 (a): this<JSpath>.length == <i>
        //		  transJS(this<JSpath>.length == <i>) :- COMPARE(<JSpath>, $size, <i>)
        if (jsExpr.getType == Token.EQ) {
            val child0 = jsExpr.children(0)
            val child1 = jsExpr.children(1)

            // One child may be the this<JSpath>.length and the other the <v>
            if ((child0.hasTypeIn(PropOrElem) && child1.getType == Token.NUMBER) ||
                (child0.getType == Token.NUMBER && child1.hasTypeIn(PropOrElem))) {

                // Find out which one is the path and which one is the integer value
                var path: JavascriptBoolExpr = null
                var value: JavascriptBoolExpr = null
                if (child0.hasTypeIn(PropOrElem)) {
                    path = child0; value = child1
                } else {
                    path = child1; value = child0
                }

                val pathSrc = path.astNode.toSource
                if (pathSrc.startsWith("this") && pathSrc.endsWith(".length") && !pathSrc.equals("this.length")) {
                    if (logger.isDebugEnabled()) logger.debug("JS expression [" + jsExpr.astNode.toSource + "] matches rule j5")
                    return new MongoQueryNodeField(
                        pathSrc.substring(4, pathSrc.indexOf(".length")), // skip the "this" and stop before the .length
                        new MongoQueryNodeCompare(
                            MongoQueryNodeCompare.Operator.SIZE,
                            value.astNode.toSource))
                }
            }
        }

        // ------ Rule j5 (b): this<JSpath>.length <op> <i>, with <op> in {!=, <=, <, >=, >}
        //		  transJS(this<JSpath>.length <op> <i>) :- WHERE(this<JSpath>.length <op> <i>)
        if (jsExpr hasTypeIn List(Token.NE, Token.LE, Token.LT, Token.GE, Token.GT)) {
            val child0 = jsExpr.children(0)
            val child1 = jsExpr.children(1)

            // One child may be the this<JSpath>.length and the other the <v>
            if ((child0.hasTypeIn(PropOrElem) && child1.getType == Token.NUMBER) ||
                (child0.getType == Token.NUMBER && child1.hasTypeIn(PropOrElem))) {

                // Find out which one is the path and which one is the integer value
                var path: JavascriptBoolExpr = null
                var value: JavascriptBoolExpr = null
                if (child0.hasTypeIn(PropOrElem)) {
                    path = child0; value = child1
                } else {
                    path = child1; value = child0
                }

                val pathSrc = path.astNode.toSource
                if (pathSrc.startsWith("this") && pathSrc.endsWith(".length") && !pathSrc.equals("this.length")) {
                    logger.warn("Unupported JS expression [" + jsExpr.astNode.toSource + "], rule j5: pattern \"this<JSpath>.length <op> <i>\" with <op> not being ==.")
                    return new MongoQueryNodeNotSupported(jsExpr.astNodeToString)
                    //return new MongoQueryNodeWhere(jsExpr.astNode.toSource)
                }
            }
        }

        // ------ Rule j6: (this<JSpath> <op> value) with <op> in {==, !=, <=, <, >=, >, =~}
        //		  transJS(this<JSpath> <op> <v>) :- COMPARE(<JSpath>, transJsOpToMongo(<op>), <v>)
        if (jsExpr hasTypeIn List(Token.EQ, Token.NE, Token.LT, Token.LE, Token.GT, Token.GE, Token.REGEXP)) {
            val child0 = jsExpr.children(0)
            val child1 = jsExpr.children(1)

            // One child may be the this<JSpath> and the other the <v>
            if ((child0.hasTypeIn(PropOrElem) && child1.hasTypeIn(Values)) ||
                (child0.hasTypeIn(Values) && child1.hasTypeIn(PropOrElem))) {

                // Find out which one is the path and which one is the value
                var path: JavascriptBoolExpr = null
                var value: JavascriptBoolExpr = null
                if (child0.hasTypeIn(PropOrElem)) {
                    path = child0; value = child1
                } else {
                    path = child1; value = child0
                }

                if (logger.isDebugEnabled()) logger.debug("JS expression [" + jsExpr.astNode.toSource + "] matches rule j6")

                val op = transJsOpToMongo(jsExpr.getType)
                if (op == null)
                    return new MongoQueryNodeNotSupported("JS operator + " + jsExpr.getType + " not supported.")
                else
                    return new MongoQueryNodeField(
                        path.astNode.toSource.substring(4), // skip the "this"
                        new MongoQueryNodeCompare(
                            transJsOpToMongo(jsExpr.getType),
                            value.astNode.toSource))
            }
        }

        // ------- Rule j7: applies when no other rule applies
        //		   transJS(<bool_expr>) :- WHERE("<bool_expr>")
        logger.warn("Unupported JS expression [" + jsExpr.astNode.toSource + "], rule j7.")
        return new MongoQueryNodeNotSupported(jsExpr.astNodeToString)
        //return new MongoQueryNodeWhere(jsExpr.astNode.toSource)
    }

    /**
     * Translate a JavaScript operator given by org.mozilla.javascript.Token
     * into an equivalent MongoDB operator
     */
    private def transJsOpToMongo(jsOp: Int): MongoQueryNodeCompare.Operator.Value = {
        jsOp match {
            case Token.EQ => MongoQueryNodeCompare.Operator.EQ
            case Token.NE => MongoQueryNodeCompare.Operator.NE
            case Token.LT => MongoQueryNodeCompare.Operator.LT
            case Token.LE => MongoQueryNodeCompare.Operator.LTE
            case Token.GT => MongoQueryNodeCompare.Operator.GT
            case Token.GE => MongoQueryNodeCompare.Operator.GTE
            case Token.REGEXP => MongoQueryNodeCompare.Operator.REGEX
            case _ => {
                logger.warn("Cannot translate JS operator + " + jsOp + " to a MongoDB operator.")
                null
            }
        }
        MongoQueryNodeCompare.Operator.EQ
    }
}