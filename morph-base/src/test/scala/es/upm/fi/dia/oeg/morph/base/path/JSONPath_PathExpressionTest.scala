package es.upm.fi.dia.oeg.morph.base.path

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import org.junit.Test

class JSONPath_PathExpressionTest {
    
    @Test def TestEvaluate() {
        println("------------------ TestEvaluate ------------------")
        val json: String = """[ 
            { "name" : "john", "gender" : "male", "age": 28},
            [ 10, "ben" ],
            { "name" : "lucie", "gender": "female"},
            ]"""

        var path = new JSONPath_PathExpression("$[*]['gender']")
        val gender = path.evaluate(json)
        println(gender)
        assertEquals("male", gender(0))
        assertEquals("female", gender(1))

        path = new JSONPath_PathExpression("$[*].age")
        val age = path.evaluate(json)
        println(age)
        assertEquals(28, age(0))

        path = new JSONPath_PathExpression("$[0]")
        val dic = path.evaluate(json)
        assertEquals(1, dic.length)
        assertEquals("""{"name":"john","gender":"male","age":28}""", dic(0))
        println(dic)

        path = new JSONPath_PathExpression("$[1]")
        val arr = path.evaluate(json)
        assertEquals(1, arr.length)
        assertEquals("""[10,"ben"]""", arr(0))
        println(arr)

        path = new JSONPath_PathExpression("$[0")
        val lst = path.evaluate(json)
        println(lst)
        assertEquals(lst, List(json))
    }
}