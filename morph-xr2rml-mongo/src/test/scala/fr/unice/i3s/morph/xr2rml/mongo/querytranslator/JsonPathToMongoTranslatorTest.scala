package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotSupported
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjectionArraySlice

class JsonPathToMongoTranslatorTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_R0() {
        println("------ test_R0")

        var jpExpr = ""
        var cond: MongoQueryNodeCond = null
        var query: MongoQueryNode = null

        jpExpr = """$"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$.*.q"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$[*].q"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$[1].q"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$[1,4].q"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$["p"].q"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertFalse(query.isInstanceOf[MongoQueryNodeNotSupported])
    }

    @Test def test_R1() {
        println("------ test_R1")

        var cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(null, cond)
        println(query.toString)
        assertEquals(cleanString("$eq: 'v'"), cleanString(query.toString))

        query = JsonPathToMongoTranslator.trans("", cond)
        println(query.toString)
        assertEquals(cleanString("$eq: 'v'"), cleanString(query.toString))

        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(null, cond)
        println(query.toString)
        assertEquals(cleanString("$exists: true, $ne: null"), cleanString(query.toString))

        query = JsonPathToMongoTranslator.trans("", cond)
        println(query.toString)
        assertEquals(cleanString("$exists: true, $ne: null"), cleanString(query.toString))
    }

    @Test def test_R2a() {
        println("------ test_R2a")

        var jpExpr = """$.p[5].q["a", "b", "c"].r"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.5.q.a.r': {$eq: 'v'}}, {'p.5.q.b.r': {$eq: 'v'}}, {'p.5.q.c.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$.p.q["a", "b", "c"]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.q.a': {$eq: 'v'}}, {'p.q.b': {$eq: 'v'}}, {'p.q.c': {$eq: 'v'}}]"), cleanString(query.toString))
    }

    @Test def test_R2b() {
        println("------ test_R2b")

        var jpExpr = """$.p[5].q[1,3,5].r"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.5.q.1.r': {$eq: 'v'}}, {'p.5.q.3.r': {$eq: 'v'}}, {'p.5.q.5.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$.p.q[1,3,5]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.q.1': {$eq: 'v'}}, {'p.q.3': {$eq: 'v'}}, {'p.q.5': {$eq: 'v'}}]"), cleanString(query.toString))
    }

    @Test def test_R3a() {
        println("------ test_R3a")

        var jpExpr = """$["a", "b", "c"].r"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'a.r': {$eq: 'v'}}, {'b.r': {$eq: 'v'}}, {'c.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$["a", "b", "c"]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'a': {$eq: 'v'}}, {'b': {$eq: 'v'}}, {'c': {$eq: 'v'}}]"), cleanString(query.toString))
    }

    @Test def test_R3b() {
        println("------ test_R3b")

        var jpExpr = """$.p[1,3,5].r"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.1.r': {$eq: 'v'}}, {'p.3.r': {$eq: 'v'}}, {'p.5.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$.p.*[1,3,5]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "v")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {$or: [{'1': {$eq: 'v'}}, {'3': {$eq: 'v'}}, {'5': {$eq: 'v'}}]}}"), cleanString(query.toString))
    }

    @Test def test_R4() {
        println("------ test_R4")

        var jpExpr = """$.p[?(@.q == 10)].r"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, java.lang.Boolean.TRUE)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'r': {$eq: true}, 'q': {$eq: 10}}}"), cleanString(query.toString))

        jpExpr = """$.p[?(@.q == 10)].r.*"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'r': {$elemMatch: {$eq: 1}}, 'q': {$eq: 10}}}"), cleanString(query.toString))

        jpExpr = """$.p[?(@.q)].r[?(@.s)].t"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'r': {$elemMatch: {'t': {$eq: 1}, 's': {$exists: true}}}, 'q': {$exists: true}}}"), cleanString(query.toString))
    }

    @Test def test_R5a() {
        println("------ test_R5a")

        var jpExpr = """$.p[-10:]"""
        var cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {$exists: true, $ne: null}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: -10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[-10:].q"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "6")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {'q': {$eq: '6'}}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: -10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[-10:].q"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "6")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond, List(new MongoQueryProjectionArraySlice("a", "99")))
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {'q': {$eq: '6'}}}"), cleanString(query.toString))
        assertEquals(cleanString("{'a': {$slice: 99}, 'p': {$slice: -10}}"), cleanString(query.toTopLevelProjection))
    }

    @Test def test_R5b() {
        println("------ test_R5b")

        var jpExpr = """$.p[0:10]"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, "6")
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {$eq: '6'}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: 10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[:10]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, "6")
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {$eq: '6'}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: 10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[:10].q"""
        cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {'q': {$exists: true, $ne: null}}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: 10}}"), cleanString(query.toTopLevelProjection))
    }

    @Test def test_R6b() {
        println("------ test_R6b")

        var jpExpr = """$.p[0][(@.length -1)]"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(2))
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0][this.p[0].length - 1] == 2'}]"), cleanString(query.toString))
    }

    @Test def test_R6c() {
        println("------ test_R6c")

        var jpExpr = """$.p[0][(@.length -1)].s"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(2))
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0][this.p[0].length - 1].s == 2'}]"), cleanString(query.toString))
    }

    @Test def test_R7() {
        println("------ test_R7")

        var jpExpr = """$.p[5].*.r"""
        var cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p.5': {$elemMatch: {'r': {$exists: true, $ne: null}}}"), cleanString(query.toString))

        jpExpr = """$.p[*][5].r"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'5.r': {$eq: 1}}}"), cleanString(query.toString))
    }

    @Test def test_R8() {
        println("------ test_R8")

        var jpExpr = """$.p[5][6].q.r"""
        var cond = new MongoQueryNodeCond(ConditionType.IsNotNull, null)
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p.5.6.q.r': {$exists: true, $ne: null}"), cleanString(query.toString))

        jpExpr = """$.p"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$eq: 1}"), cleanString(query.toString))

        jpExpr = """$["p"]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p': {$eq: 1}"), cleanString(query.toString))

        jpExpr = """$.p[5].q.r"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(1))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertEquals(cleanString("'p.5.q.r': {$eq: 1}"), cleanString(query.toString))
    }

    @Test def test_R9() {
        println("------ test_R9")

        var jpExpr = """$.p[(@.q - @.p)].*"""
        var cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(2))
        var query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$.p.*[(@.q + @.p)]"""
        cond = new MongoQueryNodeCond(ConditionType.Equals, new Integer(2))
        query = JsonPathToMongoTranslator.trans(jpExpr, cond)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])
    }
}