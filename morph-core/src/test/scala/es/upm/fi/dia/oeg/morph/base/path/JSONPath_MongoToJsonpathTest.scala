package es.upm.fi.dia.oeg.morph.base.path

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class JSONPath_MongoToJsonpathTest {

    @Test def TestEvaluate() {

        var json: String = null
        var jpExpr: String = null
        var path: JSONPath_PathExpression = null
        var eval: List[Object] = null

        json = """ { "p": 
        	{ "s": { "u": "su" }, 
              "t": { "u": "tu" } } 
            } """
        jpExpr = "$.p.['t'].u"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))
        jpExpr = "$.p.['s', 't'].u"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))

        json = """{ "p": ["valp", "valq", "valr"] }"""
        jpExpr = "$.p[?(@[0] == 'valp')]"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))
        jpExpr = "$.p[?(@.length == 3)]"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))

    }
}
