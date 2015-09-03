package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert._
import org.junit.Test

import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond

class JsonPathToMongoTranslatorTest {

    @Test def test_JPpath_ns() {
        println("------ test_JPpath_ns")

        var jpExpr = """.p.q[0].r["s"]"""
        var result = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(jpExpr, result(0).group(0))

        jpExpr = """.p.q[0,1].r"""
        result = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(".p.q", result(0).group(0))

        jpExpr = """.p[*].r"""
        result = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(".p", result(0).group(0))

        jpExpr = """.p[s]"""
        result = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(".p", result(0).group(0))

        jpExpr = """$.p"""
        result = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(jpExpr).toList
        assertEquals(0, result.length)
    }

    @Test def test_JPpath() {
        println("------ test_JPpath")

        var jpExpr = """.p[*].q[0].r["s"]"""
        var result = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(jpExpr, result(0).group(0))

        jpExpr = """.p.*"""
        result = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(jpExpr, result(0).group(0))

        jpExpr = """$.p"""
        result = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(jpExpr).toList
        assertEquals(0, result.length)
    }

    @Test def test_JPpath_Field_Alternative() {
        println("------ test_JPpath_Field_Alternative")

        var jpExpr = """["s","q"]"""
        var result = JsonPathToMongoTranslator.JSONPATH_FIELD_ALTERNATIVE.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(jpExpr, result(0).group(0))

        jpExpr = """["s"]"""
        result = JsonPathToMongoTranslator.JSONPATH_FIELD_ALTERNATIVE.findAllMatchIn(jpExpr).toList
        assertEquals(0, result.length)
    }

    @Test def test_JPpath_Quoted_Field_Name() {
        println("------ test_JPpath_Quoted_Field_Name")

        var jpExpr = """["p","q","r"]"""
        var result = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_QUOTED.findAllMatchIn(jpExpr).toList
        println(result)
        val resultL = result.toList

        assertEquals(3, result.length)
        assertEquals("p", result(0).group(1))
        assertEquals("q", result(1).group(1))
        assertEquals("r", result(2).group(1))
    }

    @Test def test_JPpath_Array_Index_Alternative() {
        println("------ test_JPpath_Array_Index_Alternative")

        var jpExpr = """[11,4,5]"""
        var result = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX_ALTERNATIVE.findAllMatchIn(jpExpr).toList
        println(result)
        assertEquals(1, result.length)
        assertEquals(jpExpr, result(0).group(0))

        jpExpr = """[2]"""
        result = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX_ALTERNATIVE.findAllMatchIn(jpExpr).toList
        assertEquals(0, result.length)

        jpExpr = """.p[1,2].q[3,4]"""
        result = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX_ALTERNATIVE.findAllMatchIn(jpExpr).toList
        assertEquals(0, result.length)
    }

    @Test def test_JPpath_Array_Index() {
        println("------ test_JPpath_Array_Index")

        var jpExpr = """[1,2,3]"""
        var result = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX.findAllMatchIn(jpExpr).toList
        println(result)
        val resultL = result.toList
        assertEquals(3, result.length)
        assertEquals("1", result(0).group(0))
        assertEquals("2", result(1).group(0))
        assertEquals("3", result(2).group(0))
    }

    @Test def test_R2a() {
        println("------ test_R2a")

        var jpExpr = """$.p[5].q["a", "b", "c"].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals("$or: [{'p.5.q.a.r': {$eq: 'v'}}, {'p.5.q.b.r': {$eq: 'v'}}, {'p.5.q.c.r': {$eq: 'v'}}]", query.toQueryString)

        jpExpr = """$.p[5].q["a", "b", "c"]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals("$or: [{'p.5.q.a': {$eq: 'v'}}, {'p.5.q.b': {$eq: 'v'}}, {'p.5.q.c': {$eq: 'v'}}]", query.toQueryString)
    }
    
        @Test def test_R2b() {
        println("------ test_R2b")

        var jpExpr = """$.p[5].q[1,3,5].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals("$or: [{'p.5.q.1.r': {$eq: 'v'}}, {'p.5.q.3.r': {$eq: 'v'}}, {'p.5.q.5.r': {$eq: 'v'}}]", query.toQueryString)

        jpExpr = """$.p[5].q[1,3,5]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals("$or: [{'p.5.q.1': {$eq: 'v'}}, {'p.5.q.3': {$eq: 'v'}}, {'p.5.q.5': {$eq: 'v'}}]", query.toQueryString)
    }
}