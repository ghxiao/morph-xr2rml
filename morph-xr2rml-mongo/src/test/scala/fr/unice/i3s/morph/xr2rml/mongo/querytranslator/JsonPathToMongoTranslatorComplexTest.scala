package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Test
import es.upm.fi.dia.oeg.morph.base.query.ConditionType
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionNotNull

class JsonPathToMongoTranslatorComplexTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_Complex() {
        println("------ test_Complex")
        var jpExpr = """$.a[?(@.q.length == 5 || @.q.length == 3)].r"""
        var cond = new AbstractQueryConditionEquals(jpExpr, "val")
        var query = JsonPathToMongoTranslator.trans(cond, None)

        println("Query unoptimized: " + query.toTopLevelQuery("tititutu"))
        println("Query optimized  : " + query.optimize.toTopLevelQuery("tititutu"))
    }

    @Test def test_ComplexUnsupportedFilter() {
        println("------ test_ComplexUnsupportedFilter")

        var jpExpr = """$.a[?( @.q.length == 5 || (@.s.length > 3 && @.t.length <= 6) )].r"""
        var cond = new AbstractQueryConditionEquals(jpExpr, "val")
        var query = JsonPathToMongoTranslator.trans(cond, None)

        println("Query optimized  : " + query.optimize.toTopLevelQuery("tititutu"))
        println("Query unoptimized: " + query.toTopLevelQuery("tititutu"))
    }

    @Test def test_ComplexPullupWhere() {
        println("------ test_ComplexPullupWhere")

        var jpExpr = """$["a","b"][1,3][(@.length-1)].r"""
        var cond = new AbstractQueryConditionEquals(jpExpr, "val")
        var query = JsonPathToMongoTranslator.trans(cond, None)

        println("Query optimized  : " + query.optimize)
        println("Query unoptimized: " + query)
    }


    @Test def test_iterator() {
        println("------ test_iterator")

        var jpExpr = """$.p"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), Some("$.ff.*"))
        println(query.toString)
        assertEquals(cleanString("'ff': {$elemMatch: {'p': {$eq: 1}}}"), cleanString(query.toString))

        jpExpr = """$.p[5].*.r"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), Some("$.ff.tt.*"))
        println(query.toString)
        assertEquals(cleanString("'ff.tt': {$elemMatch: {'p.5': {$elemMatch: {'r': {$exists: true, $ne: null}}}}}"), cleanString(query.toString))
    }

    @Test def test_Complex2() {
        println("------ test_Complex2")

        var jpExpr = """$.p.*[?(@.s)].r"""
        var cond = new AbstractQueryConditionEquals(jpExpr, "val")
        var query = JsonPathToMongoTranslator.trans(cond, None)

        println("Query optimized  : " + query.optimize.toTopLevelQuery("tititutu"))
        println("Query unoptimized: " + query.toTopLevelQuery("tititutu"))
    }
}