package es.upm.fi.dia.oeg.morph.base.path

import org.junit.Assert.assertEquals
import org.junit.Test

class TSVPath_PathExpressionTest {

    @Test def TestEvaluate() {
        println("------------------ TestEvaluate ------------------")
        
        val tsvVal = "A\tB \t C\naaa	\"bbb\"	\"ccc\"  \n '(){}'\t\"'[-|\"	D "

        var path = new TSV_PathExpression("0")
        var result = path.evaluate(tsvVal)
        println(result)
        assertEquals("A", result(0))
        assertEquals("aaa", result(1))
        assertEquals("'(){}'", result(2))

        path = new TSV_PathExpression("1")
        result = path.evaluate(tsvVal)
        println(result)
        assertEquals("B", result(0))
        assertEquals("bbb", result(1))
        assertEquals("'[-|", result(2))
    }
}