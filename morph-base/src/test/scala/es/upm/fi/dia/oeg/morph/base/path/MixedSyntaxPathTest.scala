package es.upm.fi.dia.oeg.morph.base.path

import scala.collection.mutable.Queue
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import es.upm.fi.dia.oeg.morph.base.Constants
import com.jayway.jsonpath.ReadContext

class MixedSyntaxPathTest {

    @Test def TestRegex1() {
        println("------------------ TestRegex1 ------------------")
        // Following characters must be escaped: /(){} => (\/) (\() (\)) (\{) (\})
        // Following characters must not be escaped: !#%&,-./:;<=>?@_`|~[]"'*+^$
        val pathConstructors = "(Column|XPath|JSONPath|CSV|TSV)"
        val pathChars = """([\p{Alnum}\p{Space}!#%&,-.:;<=>?(\\@)_`\|~\[\]\"\'\*\+\^\$]|(\\/)|(\\\()|(\\\)|(\\\{)|(\\\})))+"""
        val pathRegex = "(" + pathConstructors + """\(""" + pathChars + """\))""";

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"

        var lst = pathRegex.r.findAllMatchIn(xPath).toList
        println("Parse XPath: " + lst)
        assertTrue(lst.length == 1)

        lst = pathRegex.r.findAllMatchIn(jsonPath).toList
        println("Parse JSONPath: " + lst)
        assertTrue(lst.length == 1)

        lst = pathRegex.r.findAllMatchIn(mixedPath).toList
        println("Parse mixed syntax path: " + lst)
        assertTrue(lst.length == 5)

        lst = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX.findAllMatchIn(mixedPath).toList
        println("Parse mixed syntax path: " + lst)
        assertTrue(lst.length == 5)

        lst = pathRegex.r.findAllMatchIn("NAME").toList
        println("Parse expr without path constructor: " + lst);
        assertTrue(lst.length == 0)
    }

    @Test def TestRegex2() {
        println("------------------ TestRegex2 ------------------")
        val pathRegex = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"

        var isOk = xPath match {
            case pathRegex(_*) => " is a mixed syntax path"
            case _ => " is not a mixed syntax path"
        }
        assertEquals(isOk, " is a mixed syntax path")
        println(xPath + isOk)

        isOk = "NAME" match {
            case pathRegex(_*) => " is a mixed syntax path"
            case _ => " is not a mixed syntax path"
        }
        println("NAME" + isOk)
        assertEquals(isOk, " is not a mixed syntax path")
    }

    @Test def TestRegex3() {
        println("------------------ TestRegex3 ------------------")
        val pathRegex = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"

        var colRef = MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI).getReferencedColumn
        println("Column references: " + colRef)
        assertEquals("NAME", colRef.get)
    }

    /**
     * This test actually tests the code of method TemplateUtil.replaceTemplateTokens()
     */
    @Test def TestRegex4() {
        println("------------------ TestRegex4 ------------------")

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"

        // ------------------------------------------------------------------------------------------

        var tpl = "http://example.org/student/{ID}/{" + mixedPath + "}/{ID2}/{" + mixedPath + "}"

        val mixedSntxRegex = xR2RML_Constants.xR2RML_MIXED_SYNTX_PATH_REGEX

        // Save all path expressions in the template string
        val mixedSntxPaths: Queue[String] = Queue(mixedSntxRegex.findAllMatchIn(tpl).toList.map(item => item.toString): _*)
        println("mixedSntxPaths: " + mixedSntxPaths)

        // Replace each path expression with a place holder "xR2RML_replacer"
        tpl = mixedSntxRegex.replaceAllIn(tpl, "xR2RML_replacer")
        println("tpl: " + tpl)

        // Make a list of the R2RML template groups between '{' '}'
        val listPattern = Constants.R2RML_TEMPLATE_PATTERN.r.findAllIn(tpl).toList
        println("listPattern: " + listPattern)

        // Restore the path expressions in each of the place holders
        val listReplaced = MixedSyntaxPath.replaceTplPlaceholder(listPattern, mixedSntxPaths)
        println("Liste finale: " + listReplaced)

        // Extract the column references of each template group between '{' and '}'
        val colRefs = listReplaced.map(group =>
            {
                val col = MixedSyntaxPath(group, xR2RML_Constants.xR2RML_COLUMN_URI).getReferencedColumn.getOrElse("")
                // For simple columns there has been no parsing at all so they still have the '{' and '}'
                if (col.startsWith("{") && col.endsWith("}"))
                    col.substring(1, col.length() - 1)
                else col
            }
        )

        println("Column references: " + colRefs)
        assertEquals("ID", colRefs(0))
        assertEquals("NAME", colRefs(1))
        assertEquals("ID2", colRefs(2))
        assertEquals("NAME", colRefs(3))
    }

    @Test def TestReplaceTplPlaceholder() {
        println("------------------ TestReplaceTplPlaceholder ------------------")
        val tpl = List("{ID}", "{xR2RML_replacer}", "{ID2}", "{xR2RML_replacer}")
        val replacements = Queue("Column(A)", "JSONPath($.store.book[0].title.['title'])")
        val repl = MixedSyntaxPath.replaceTplPlaceholder(tpl, replacements)
        println("List with replacements: " + repl)
        assertEquals("{ID}", repl(0))
        assertEquals("{Column(A)}", repl(1))
        assertEquals("{ID2}", repl(2))
        assertEquals("{JSONPath($.store.book[0].title.['title'])}", repl(3))
    }

    @Test def TestEvalJsonPath() {
        println("------------------ TestEvalJsonPath ------------------")
        val json: String = """[ 
            { "name" : "john", "gender" : "male", "age": 28},
            [ 10, "ben" ],
            { "name" : "lucie", "gender": "female"},
            ]"""

        var gender = MixedSyntaxPath.evalJSONPath(json, "$[*]['gender']")
        println(gender)
        assertEquals("male", gender(0))
        assertEquals("female", gender(1))

        val age = MixedSyntaxPath.evalJSONPath(json, "$[*].age")
        println(age)
        assertEquals(28, age(0))

        var dic = MixedSyntaxPath.evalJSONPath(json, "$[0]")
        assertEquals(1, dic.length)
        assertEquals("""{"name":"john","gender":"male","age":28}""", dic(0))
        println(dic)

        var arr = MixedSyntaxPath.evalJSONPath(json, "$[1]")
        assertEquals(1, arr.length)
        assertEquals("""[10,"ben"]""", arr(0))
        println(arr)

        var lst = MixedSyntaxPath.evalJSONPath(json, "$[0")
        println(lst)
        assertTrue(lst.isEmpty)
    }

    @Test def TestReconstructMixedSyntaxPath() {
        println("------------------ TestReconstructMixedSyntaxPath ------------------")

        var pathToStr = MixedSyntaxPath("NAME", xR2RML_Constants.xR2RML_COLUMN_URI).reconstructMixedSyntaxPath
        println("Reconstructred path: " + pathToStr)
        assertEquals("Column(NAME)", pathToStr)

        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"
        pathToStr = MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI).reconstructMixedSyntaxPath
        println("Reconstructred path: " + pathToStr)
        assertEquals(MixedSyntaxPath.unescapeChars(mixedPath), pathToStr)
    }

    @Test def TestEvaluate() {
        println("------------------ TestEvaluate ------------------")
        
        var paths = MixedSyntaxPath("NAME", xR2RML_Constants.xR2RML_COLUMN_URI)
        var result = paths.evaluate("one simple value") 
        println("Eval: " + result)
        assertEquals("one simple value", result(0))

        var jsonValue: String = """[{ "name" : "john", "age": 28},{ "name" : "lucie", "isMale": false}]"""
        var mixedPath = "Column(NAME)/JSONPath($.*.*)"
        paths = MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI)
        result = paths.evaluate(jsonValue)
        println("Eval: " + result)
        assertEquals(List("john", 28, "lucie", false), result)
    }
}