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
        jpExpr = "$.p.['s', 't']"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))

        json = """{ "p": ["valp", "valq", "valr"] }"""
        jpExpr = "$.p[?(@ == 'valp')]"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))
        jpExpr = "$[?(@.length == 3)]"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))

        json = """{ 
            "a": [ {"p1": "valp1", p2: "valp2"} ], 
            "b": [ {"q1": "valq1", q2: "valq2"} ]
            }"""
        jpExpr = "$.*[?(@.q1)].q2"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))
        
        
        json = """{ 
            "a": [ {"p": "val", q: "val", r: "valr1"}, {"p": "valp2", "q": "valq2", "r": "valr2"} ]
            }"""
        jpExpr = "$.a[?(@.p == @.q)].r"
        println(new JSONPath_PathExpression(jpExpr).evaluate(json))
        
    }
}
