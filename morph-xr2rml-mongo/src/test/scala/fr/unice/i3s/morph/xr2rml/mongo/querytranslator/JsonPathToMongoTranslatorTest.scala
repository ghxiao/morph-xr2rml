package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond

class JsonPathToMongoTranslatorTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_R2a() {
        println("------ test_R2a")

        var jpExpr = """$.p[5].q["a", "b", "c"].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'p.5.q.a.r': {$eq: 'v'}}, {'p.5.q.b.r': {$eq: 'v'}}, {'p.5.q.c.r': {$eq: 'v'}}]"), cleanString(query.toQueryString))

        jpExpr = """$.p.q["a", "b", "c"]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'p.q.a': {$eq: 'v'}}, {'p.q.b': {$eq: 'v'}}, {'p.q.c': {$eq: 'v'}}]"), cleanString(query.toQueryString))
    }

    @Test def test_R2b() {
        println("------ test_R2b")

        var jpExpr = """$.p[5].q[1,3,5].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'p.5.q.1.r': {$eq: 'v'}}, {'p.5.q.3.r': {$eq: 'v'}}, {'p.5.q.5.r': {$eq: 'v'}}]"), cleanString(query.toQueryString))

        jpExpr = """$.p.q[1,3,5]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'p.q.1': {$eq: 'v'}}, {'p.q.3': {$eq: 'v'}}, {'p.q.5': {$eq: 'v'}}]"), cleanString(query.toQueryString))
    }

    @Test def test_R3a() {
        println("------ test_R3a")

        var jpExpr = """$["a", "b", "c"].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'a.r': {$eq: 'v'}}, {'b.r': {$eq: 'v'}}, {'c.r': {$eq: 'v'}}]"), cleanString(query.toQueryString))

        jpExpr = """$["a", "b", "c"]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'a': {$eq: 'v'}}, {'b': {$eq: 'v'}}, {'c': {$eq: 'v'}}]"), cleanString(query.toQueryString))
    }

    @Test def test_R3b() {
        println("------ test_R3b")

        var jpExpr = """$[1,3,5].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'1.r': {$eq: 'v'}}, {'3.r': {$eq: 'v'}}, {'5.r': {$eq: 'v'}}]"), cleanString(query.toQueryString))

        jpExpr = """$[1,3,5]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$or: [{'1': {$eq: 'v'}}, {'3': {$eq: 'v'}}, {'5': {$eq: 'v'}}]"), cleanString(query.toQueryString))
    }

    @Test def test_R4() {
        println("------ test_R4")

        var jpExpr = """$.p[?(@.q == 10)].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, java.lang.Boolean.TRUE)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$and: [{'p.r': {$eq: true}}, {'p.q': {$eq: 10}}]"), cleanString(query.toQueryString))
    }

    @Test def test_R5() {
        println("------ test_R5")

        var jpExpr = """$[?(@.q == 10)].r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, java.lang.Boolean.TRUE)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$and: [{'r': {$eq: true}}, {'q': {$eq: 10}}]"), cleanString(query.toQueryString))

        jpExpr = """$[?(@.p[5] >= @.q)].r"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, java.lang.Boolean.TRUE)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertTrue(cleanString(query.toQueryString).startsWith(cleanString("$and: [{'r': {$eq: true}}, {$and: [{")))
        assertTrue(cleanString(query.toQueryString).contains(cleanString("{'p.5': {$exists: true}}")))
        assertTrue(cleanString(query.toQueryString).contains(cleanString("{$where: 'this.p[5] >= this.q'}")))
    }

    @Test def test_R6a() {
        println("------ test_R6a")

        var jpExpr = """$.p[(@.q + @.r)]"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(2))
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$and: [{'p': {$exists: true}}, {$where: 'this.p[this.p.q + this.p.r] == 2'}]"), cleanString(query.toQueryString))

        jpExpr = """$.p[0][(@.length -1)]"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(2))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0][this.p[0].length - 1] == 2'}]"), cleanString(query.toQueryString))
    }

    @Test def test_R6b() {
        println("------ test_R6b")

        var jpExpr = """$.p[(@.q + @.r)].s"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(2))
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$and: [{'p': {$exists: true}}, {$where: 'this.p[this.p.q + this.p.r].s == 2'}]"), cleanString(query.toQueryString))

        jpExpr = """$.p[0][(@.length -1)].s"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(2))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0][this.p[0].length - 1].s == 2'}]"), cleanString(query.toQueryString))
    }

    @Test def test_R7_R8() {
        println("------ test_R7_R8")

        var jpExpr = """$.p[5].q.r"""
        var cond = new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("'p.5.q.r': {$exists: true, $ne: null}"), cleanString(query.toQueryString))

        jpExpr = """$.p[5].q.r"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("'p.5.q.r': {$eq: 1}"), cleanString(query.toQueryString))

        jpExpr = """$.p[5].*.r"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("'p.5': {$elemMatch: {'r': {$exists: true, $ne: null}}}"), cleanString(query.toQueryString))

        jpExpr = """$.p[*][5].r"""
        cond = new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toQueryString)
        assertEquals(cleanString("'p': {$elemMatch: {'5.r': {$eq: 1}}}"), cleanString(query.toQueryString))
    }
}