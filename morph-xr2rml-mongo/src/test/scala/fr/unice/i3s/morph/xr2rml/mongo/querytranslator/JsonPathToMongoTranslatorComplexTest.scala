package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotSupported
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphConditionType

class JsonPathToMongoTranslatorComplexTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_Complex() {
        println("------ test_Complex")
        var jpExpr = """$.a[?(@.q.length == 5 || @.q.length == 3)].r"""
        var cond = new MongoQueryNodeCond(MorphConditionType.Equals, "val")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)

        println("Query optimized  : " + query.optimize.toTopLevelQuery)
    }

    @Test def test_ComplexUnsupportedFilter() {
        println("------ test_ComplexUnsupportedFilter")

        var jpExpr = """$.a[?( @.q.length == 5 || (@.s.length > 3 && @.t.length <= 6) )].r"""
        var cond = new MongoQueryNodeCond(MorphConditionType.Equals, "val")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)

        println("Query optimized  : " + query.optimize.toTopLevelQuery)
        println("Query unoptimized: " + query.toTopLevelQuery)
    }

    @Test def test_ComplexPullupWhere() {
        println("------ test_ComplexPullupWhere")

        var jpExpr = """$["a","b"][1,3][(@.length-1)].r"""
        var cond = new MongoQueryNodeCond(MorphConditionType.Equals, "val")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)

        println("Query optimized  : " + query.optimize.toTopLevelQuery)
        println("Query unoptimized: " + query.toTopLevelQuery)
    }

    @Test def test_Complex2() {
        println("------ test_Complex2")

        var jpExpr = """$.p.*[?(@.s)].r"""
        var cond = new MongoQueryNodeCond(MorphConditionType.Equals, "val")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)

        println("Query optimized  : " + query.optimize.toTopLevelQuery)
        println("Query unoptimized: " + query.toTopLevelQuery)
    }
}