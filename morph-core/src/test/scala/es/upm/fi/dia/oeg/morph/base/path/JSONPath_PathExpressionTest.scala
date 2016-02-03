package es.upm.fi.dia.oeg.morph.base.path

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
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

    @Test def TestEvaluate2() {
        println("------------------ TestEvaluate2 ------------------")
        val json: String = """[ 
            { "name" : "john", "gender" : "male", "age": 28},
            [ 10, "ben" ],
            { "name" : "lucie", "gender": "female"},
            ]"""

        var path = new JSONPath_PathExpression("$[?(@.gender == 'male')].age")
        val result = path.evaluate(json)
        println(result)
    }

    @Test def TestEvaluate3() {
        println("------------------ TestEvaluate3 ------------------")
        val json: String = """
        	{"p": { "name" : "john", "gender" : "male", "age": 28}},
            """

        var path = new JSONPath_PathExpression("$.p[?(@.gender == 'male')][?(@.name == 'john')].age")
        var result = path.evaluate(json)
        println(result)
        
        path = new JSONPath_PathExpression("$.p[?(@.gender == 'male' && @.name == 'john')].age")
        result = path.evaluate(json)
        println(result)
    }

    @Test def TestEvaluate4() {
        println("------------------ TestEvaluate4 ------------------")
        val json: String = """
        	{ "dept":"Sales",
              "code":"sa",
              "manager":"F. Underwood",
        	  "members":[{"name":"mark", "gender":"male", "age":40}, {"name":"sophie", "gender":"female", "age":23}]}
            """

        var path = new JSONPath_PathExpression("$.members[?(@.age >= 40)].name")
        var result = path.evaluate(json)
        println(result)        
    }

}