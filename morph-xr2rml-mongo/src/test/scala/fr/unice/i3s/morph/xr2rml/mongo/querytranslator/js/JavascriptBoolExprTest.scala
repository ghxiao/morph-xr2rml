package fr.unice.i3s.morph.xr2rml.mongo.querytranslator.js

import org.junit.Assert.assertNull
import org.junit.Test
import org.mozilla.javascript.CompilerEnvirons
import org.mozilla.javascript.Context
import org.mozilla.javascript.ErrorReporter
import org.mozilla.javascript.EvaluatorException
import org.mozilla.javascript.Parser
import org.mozilla.javascript.Scriptable
import org.mozilla.javascript.Token
import org.mozilla.javascript.ast.AstNode
import org.mozilla.javascript.ast.AstRoot
import org.mozilla.javascript.ast.NodeVisitor

class JavascriptBoolExprTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    /**
     * Just a debugging visitor class to trace nodes produced by the parser of Rhino
     */
    class Visitor extends NodeVisitor {
        def visit(ast: AstNode): Boolean = {
            var retrait = ""
            for (i <- 0 to ast.depth())
                retrait = retrait + "  "
            println(retrait + "Type         : " + Token.typeToName(ast.getType()))
            println(retrait + "TypeNum      : " + ast.getType)
            println(retrait + "hasChildren  : " + ast.hasChildren())
            println(retrait + "toSource     : " + ast.toSource())
            println(retrait + "getPosition  : " + ast.getPosition())
            println(retrait + "getLength    : " + ast.getLength())
            println(retrait + "className    : " + ast.shortName())
            println(retrait + "hashCode     : " + ast.hashCode())
            if (ast.getParent() != null) println(retrait + "Parent type  : " + ast.getParent().getType)
            println;
            true
        }
    }

    class JsParseErrorReporter extends ErrorReporter {
        def warning(message: String, sourceName: String, line: Int, lineSource: String, lineOffset: Int): Unit = {
            println("*** Warning: " + message)
        }

        def error(message: String, sourceName: String, line: Int, lineSource: String, lineOffset: Int): Unit = {
            println("*** Error: " + message)
        }

        def runtimeError(message: String, sourceName: String, line: Int, lineSource: String, lineOffset: Int): EvaluatorException = {
            new EvaluatorException(message, sourceName, line)
        }
    }

    @Test def test_JS() {
        println("------------------------------------------------- test_JS")
        val jsExpr = """(true && this[1] && ! this.q1.q2 && this.r == "xyz" && (this.s + this.p[1]) == "thy") || ("ujy" == "ujy")""";
        val cx: Context = Context.enter()
        val scope: Scriptable = cx.initStandardObjects()

        val result = cx.evaluateString(scope, jsExpr, "", 1, null);
        println(result)

        // Compile in one line
        val compiled = cx.compileString(jsExpr, null, 1, null)

        // Compile using the classes of Rhino
        val compilerEnv: CompilerEnvirons = new CompilerEnvirons()
        compilerEnv.initFromContext(cx)
        val p: Parser = new Parser(compilerEnv, new JsParseErrorReporter)
        val ast: AstRoot = p.parse(jsExpr, null, 1);
        ast.visit(new Visitor)
        //println("AstRoot:" + ast.debugPrint())
    }

    @Test def test_JS2() {
        println("------------------------------------------------- test_JS2 ")

        //"""(true && this[1] && ! this.q1.q2 && this.r == "xyz" && (this.s + this.p[1]) == "thy") || ("ujy" == "ujy")""";
        var jsExpr: String = null
        var absJs: JavascriptBoolExpr = null

        jsExpr = """(this.q && this.p[1]""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        assertNull(absJs)	// erroneous JS expression

        jsExpr = """(this.q && this.p[1] && ! this.q1.q2)""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)

        jsExpr = """this.r == "xyz"""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)
        jsExpr = """10 == this.r""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)
        jsExpr = """this.r == false""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)

        jsExpr = """this.p.length == 10""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)

        jsExpr = """this.s + this.p[1] == "thy"""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)

        jsExpr = """(this.s == "thy") || (3 <= 4)""";
        absJs = JavascriptBoolExprFactory.create(jsExpr).getOrElse(null)
        println(absJs)
    }
}
