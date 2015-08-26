package es.upm.fi.dia.oeg.morph.base.path

import org.junit.Assert.assertEquals
import org.junit.Test

class CSVPath_PathExpressionTest {

    @Test def TestEvaluate() {
        println("------------------ TestEvaluate ------------------")

        val csvVal = """A,B,C
            aaa, "bbb"    , ccc
        	"'(){}'", "'[-|", D"""

        var path = new CSV_PathExpression("0")
        var result = path.evaluate(csvVal)
        println(result)
        assertEquals("A", result(0))
        assertEquals("aaa", result(1))
        assertEquals("'(){}'", result(2))

        path = new CSV_PathExpression("1")
        result = path.evaluate(csvVal)
        println(result)
        assertEquals("B", result(0))
        assertEquals("bbb", result(1))
        assertEquals("'[-|", result(2))

    }
}