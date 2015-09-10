package fr.unice.i3s.morph.xr2rml.mongo.querytranslator.js

import scala.collection.mutable.HashMap

import org.apache.log4j.Logger
import org.mozilla.javascript.CompilerEnvirons
import org.mozilla.javascript.Context
import org.mozilla.javascript.ErrorReporter
import org.mozilla.javascript.EvaluatorException
import org.mozilla.javascript.Parser
import org.mozilla.javascript.Token
import org.mozilla.javascript.ast.AstNode
import org.mozilla.javascript.ast.AstRoot
import org.mozilla.javascript.ast.NodeVisitor

/**
 * This utility object creates abstract representations of JavaScript boolean expressions, based on the Rhino AstNode class
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
object JavascriptBoolExprFactory {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Result of parsing a JS expression. This member is reinitialized and populated
     * each time the create method is invoked.
     */
    var nodes: HashMap[Int, JavascriptBoolExpr] = null

    // Rhino JS parser initialization
    val cx: Context = Context.enter()
    val compilerEnv: CompilerEnvirons = new CompilerEnvirons()
    compilerEnv.initFromContext(cx)
    var jsParser: Parser = null

    private def init() {
        nodes = new HashMap[Int, JavascriptBoolExpr]
        jsParser = new Parser(compilerEnv, new JsParseErrorReporter)
    }

    /**
     * The NodeVisitor class is the only way to browse the nodes created by Rhino when parsing a JS expression.
     */
    class Visitor extends NodeVisitor {
        def visit(ast: AstNode): Boolean = {
            nodes += (ast.hashCode() -> new JavascriptBoolExpr(ast))
            true
        }
    }

    /**
     * The ErrorReporter class is the way to capture error events while Rhino parses a JS expression.
     */
    class JsParseErrorReporter extends ErrorReporter {
        def warning(message: String, sourceName: String, line: Int, lineSource: String, lineOffset: Int): Unit = {
            logger.warn("JS expression parsing returned: " + message)
        }
        def error(message: String, sourceName: String, line: Int, lineSource: String, lineOffset: Int): Unit = {
            logger.warn("JS expression parsing returned: " + message)
        }
        def runtimeError(message: String, sourceName: String, line: Int, lineSource: String, lineOffset: Int): EvaluatorException = {
            new EvaluatorException(message, sourceName, line)
        }
    }

    /**
     * Create a set of AbstractJsExpr that mimics the result of parsing the JavaScript expression by Rhino.
     * In addition parent-child links are set and unneeded Rhino nodes are removed
     * (unneeded in our specific context of boolean JS expressions)
     *
     * @param jsExpr a JavaScript boolean expression
     * @return the root node corresponding to the parsed expression, null in case any error occurs
     */
    def create(jsExpr: String): Option[JavascriptBoolExpr] = {
        try {
            init()
            val rootNode: AstRoot = jsParser.parse(jsExpr, null, 1)
            if (!rootNode.hasChildren())
                logger.warn("No children to the JavaScript expression " + rootNode.toSource() + ". Will be passed to the database as is.")
            else {
                // Initialize the map "nodes" with all AstNode found in the JS expression
                rootNode.visit(new Visitor)
                if (logger.isTraceEnabled()) logger.trace("JavaScript expression: " + astNodeToString(rootNode))

                // Remove unneeded nodes: capturing parentheses and expression statement
                for (n <- nodes.values) {
                    if (n.astNode.getType() == Token.EXPR_RESULT || n.astNode.getType() == Token.LP)
                        removeAstNode(n)
                }

                // Make links from parent to children
                for (n <- nodes.values) {
                    if (n.parentAstNode != null)
                        nodes.get(n.parentAstNode.hashCode).get.children += n
                }
            }
            nodes.get(rootNode.hashCode)
        } catch {
            case e: EvaluatorException => {
                logger.error("Exception raised during the parsing of JS expression: [" + jsExpr + "]. Returing None")
                None
            }
        }

    }

    /**
     * Remove one node from the list of JS nodes, and changes pointers of children of that node
     * to their grand parent
     */
    private def removeAstNode(node: JavascriptBoolExpr) {
        if (nodes.keySet.contains(node.id)) {
            for (n <- nodes.values)
                // For all children of node, reset their parent to node's parent, i.e. their grand-parent
                if (n.id != node.id && n.parentAstNode != null && n.parentAstNode.hashCode == node.id) {
                    n.astNode.setParent(node.parentAstNode)
                    n.parentAstNode = node.parentAstNode
                }

            // Then remove node
            nodes.remove(node.id)
            if (logger.isTraceEnabled()) logger.trace("Removed Javascript node: " + node.astNodeToString)
        } else
            logger.warn("Can't remove Javascript node: " + node.astNodeToString)
    }

    private def astNodeToString(ast: AstNode) = { Token.typeToName(ast.getType) + ": " + ast.toSource }

}