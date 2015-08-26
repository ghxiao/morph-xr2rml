package es.upm.fi.dia.oeg.morph.base.path

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class XPath_PathExpressionTest {

    @Test def TestEvalaluate() {

        println("------------------ TestEvaluate ------------------")

        val xmlStr = """
		<Employees>
			<Employee emplid="1111" type="admin">
				<firstname>John</firstname>
				<lastname>Watson</lastname>
				<age>30</age>
				<email>johnwatson@sh.com</email>
			</Employee>            
			<Employee emplid="2222">
				<firstname>Paul</firstname>
				<email>boo@foo.com</email>
				<age>40</age>
			</Employee>
		</Employees>"""

        // Result = 2 XML elements
        var result = new XPath_PathExpression("""//Employee/age""").evaluate(xmlStr)
        println(result)
        assertEquals(List("30", "40"), result)

        // Result = values of attribute
        result = new XPath_PathExpression("""//Employee/@emplid""").evaluate(xmlStr)
        println(result)
        assertEquals(List("1111", "2222"), result)

        // Result = serialized XML subtree
        println(new XPath_PathExpression("""//Employee""").evaluate(xmlStr))
        result = new XPath_PathExpression("""//Employee""").evaluate(xmlStr)
        println(result)
        assertTrue(result(0).toString.contains("<age>30</age><email>johnwatson@sh.com</email></Employee>"))
        assertTrue(result(1).toString.contains("""<Employee emplid="2222"><firstname>Paul</firstname><email>boo@foo.com</email>"""))

        // Result = nothing
        result = new XPath_PathExpression("""//foo""").evaluate(xmlStr)
        println(result)
        assertEquals(List(), result)

        // Error in the XPath => return the value as is
        result = new XPath_PathExpression("""/dsff/[dfsf}/foo""").evaluate(xmlStr)
        println(result)
        // Here the data is evaluated against expression "/", which is the default expression when the one passed is invalid.
        // So the serialized result contains the usual xml header
        assertEquals(result, List("""<?xml version="1.0" encoding="UTF-8" standalone="no"?>""" + onOneLine(xmlStr)))

        // Error in the XML => return the value as is
        result = new XPath_PathExpression("""/foo""").evaluate(xmlStr + "<ggdf")
        println(result)
        // Here the data cannot be evaluated, so it is simply returned after replacing CR and blank spaces.
        // Thus, there is no xml header as above
        assertEquals(result, List(onOneLine(xmlStr + "<ggdf")))
    }

    def onOneLine(str: String): String = {
        str.trim.replace("\n", "").replace("\r", "").replaceAll(">[ \t]+<", "><")
    }
}