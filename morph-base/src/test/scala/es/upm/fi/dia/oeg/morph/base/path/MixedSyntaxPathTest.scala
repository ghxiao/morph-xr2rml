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

    @Test def TestReferencedColumn() {
        println("------------------ TestReferencedColumn ------------------")
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

    @Test def TestEvaluateColJson() {
        println("------------------ TestEvaluateColJson ------------------")

        var paths = MixedSyntaxPath("NAME", xR2RML_Constants.xR2RML_COLUMN_URI)
        var result = paths.evaluate("one simple value")
        println("Eval: " + result)
        assertEquals("one simple value", result(0))

        // JSON value read from an RDB
        var jsonValue: String = """[{ "name" : "john", "age": 28}, { "name" : "lucie", "isMale": false}]"""
        var mixedPath = "Column(NAME)/JSONPath($.*.*)"
        paths = MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI)
        result = paths.evaluate(jsonValue)
        println("Eval: " + result)
        assertEquals(List("john", 28, "lucie", false), result)
    }

    @Test def TestEvaluateColXpath() {
        println("------------------ TestEvaluateColXpath ------------------")

        // XML value read from an RDB
        //var value: String = """[{ "name" : "john", "age": 28}, { "name" : "lucie", "isMale": false}]"""

        val value = """
            <People>
	            <Person id="John1" type="admin">
	        		<firstname>John</firstname>
	        		<lastname>Watson</lastname>
	        		<age>30</age>
	        		<email>johnwatson@sh.com</email>
	    		</Person>            
	            <Person id="2222">
	        		<firstname>Paul</firstname>
	        		<email>boo@foo.com</email>
	        		<age>40</age>
	    		</Person>"
	            <Person id="Abou">
	        		<firstname>Abou</firstname>
	    		</Person>"
            </People>"""

        var mixedPath = """Column(NAME)/XPath(\/\/Person[email]\/firstname)"""
        val paths = MixedSyntaxPath(mixedPath, xR2RML_Constants.xR2RML_COLUMN_URI)
        println(paths.toString)
        val result = paths.evaluate(value)
        println("Eval: " + result)
        assertEquals(List("John", "Paul"), result)
    }

    @Test def TestEvaluateJsonXpath() {
        println("------------------ TestEvaluateJsonXpath ------------------")

        // JSON value embedded in a XML value
        val value = """
            <People>
	            <Person id="John1" type="admin">
	        		<details>
            			{ "firstname" : "John", "lastname": "Watson", "age": 28}
            		</details>
	        		<email>johnwatson@sh.com</email>
	    		</Person>
	            <Person id="2222">
	        		<details>
            			{ "firstname" : "Lucie", "gender": "female", "age": 34}
            		</details>
	        		<email>boo@foo.com</email>
	    		</Person>"
            </People>"""

        var mixedPath = """XPath(\/\/Person\/details)/JSONPath($.firstname)"""
        val paths = MixedSyntaxPath(mixedPath, xR2RML_Constants.QL_XPATH_URI)
        println(paths.toString)
        val result = paths.evaluate(value)
        println("Eval: " + result)
        assertEquals(List("John", "Lucie"), result)
    }
}