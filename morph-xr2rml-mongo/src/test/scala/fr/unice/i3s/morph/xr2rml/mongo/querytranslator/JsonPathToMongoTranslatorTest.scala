package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionIsNull
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionNotNull
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotSupported
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjectionArraySlice

class JsonPathToMongoTranslatorTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_R0() {
        println("------ test_R0")

        var jpExpr = ""
        var cond: AbstractQueryConditionNotNull = null
        var query: MongoQueryNode = null

        jpExpr = """$"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$.*.q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$[*].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$[1].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$[1,4].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$["p"].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertFalse(query.isInstanceOf[MongoQueryNodeNotSupported])
    }

    @Test def test_R1() {
        println("------ test_R1")

        var cond = new AbstractQueryConditionEquals(null, "v")
        var query = JsonPathToMongoTranslator.trans(cond, None)
        println(query.toString)
        assertEquals(cleanString("$eq: 'v'"), cleanString(query.toString))

        cond = new AbstractQueryConditionEquals("", "v")
        query = JsonPathToMongoTranslator.trans(cond, None)
        println(query.toString)
        assertEquals(cleanString("$eq: 'v'"), cleanString(query.toString))

        var cond2 = new AbstractQueryConditionNotNull(null)
        query = JsonPathToMongoTranslator.trans(cond2, None)
        println(query.toString)
        assertEquals(cleanString("$exists: true, $ne: null"), cleanString(query.toString))

        cond2 = new AbstractQueryConditionNotNull("")
        query = JsonPathToMongoTranslator.trans(cond2, None)
        println(query.toString)
        assertEquals(cleanString("$exists: true, $ne: null"), cleanString(query.toString))
    }

    @Test def test_R2a() {
        println("------ test_R2a")

        var jpExpr = """$.p[5].q["a", "b", "c"].r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.5.q.a.r': {$eq: 'v'}}, {'p.5.q.b.r': {$eq: 'v'}}, {'p.5.q.c.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$.p.q["a", "b", "c"]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.q.a': {$eq: 'v'}}, {'p.q.b': {$eq: 'v'}}, {'p.q.c': {$eq: 'v'}}]"), cleanString(query.toString))
    }

    @Test def test_R2b() {
        println("------ test_R2b")

        var jpExpr = """$.p[5].q[1,3,5].r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.5.q.1.r': {$eq: 'v'}}, {'p.5.q.3.r': {$eq: 'v'}}, {'p.5.q.5.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$.p.q[1,3,5]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.q.1': {$eq: 'v'}}, {'p.q.3': {$eq: 'v'}}, {'p.q.5': {$eq: 'v'}}]"), cleanString(query.toString))
    }

    @Test def test_R3a() {
        println("------ test_R3a")

        var jpExpr = """$["a", "b", "c"].r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'a.r': {$eq: 'v'}}, {'b.r': {$eq: 'v'}}, {'c.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$["a", "b", "c"]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'a': {$eq: 'v'}}, {'b': {$eq: 'v'}}, {'c': {$eq: 'v'}}]"), cleanString(query.toString))
    }

    @Test def test_R3b() {
        println("------ test_R3b")

        var jpExpr = """$.p[1,3,5].r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("$or: [{'p.1.r': {$eq: 'v'}}, {'p.3.r': {$eq: 'v'}}, {'p.5.r': {$eq: 'v'}}]"), cleanString(query.toString))

        jpExpr = """$.p.*[1,3,5]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "v"), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {$or: [{'1': {$eq: 'v'}}, {'3': {$eq: 'v'}}, {'5': {$eq: 'v'}}]}}"), cleanString(query.toString))
    }

    @Test def test_R4() {
        println("------ test_R4")

        var jpExpr = """$.p[?(@.q == 10)].r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, java.lang.Boolean.TRUE), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'r': {$eq: true}, 'q': {$eq: 10}}}"), cleanString(query.toString))

        jpExpr = """$.p[?(@.q == 10)].r.*"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'r': {$elemMatch: {$eq: 1}}, 'q': {$eq: 10}}}"), cleanString(query.toString))

        jpExpr = """$.p[?(@.q)].r[?(@.s)].t"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'r': {$elemMatch: {'t': {$eq: 1}, 's': {$exists: true}}}, 'q': {$exists: true}}}"), cleanString(query.toString))
    }

    @Test def test_R5a() {
        println("------ test_R5a")

        var jpExpr = """$.p[-10:]"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {$exists: true, $ne: null}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: -10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[-10:].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "6"), None)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {'q': {$eq: '6'}}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: -10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[-10:].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "6"), None, List(new MongoQueryProjectionArraySlice("a", "99")))
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {'q': {$eq: '6'}}}"), cleanString(query.toString))
        assertEquals(cleanString("{'a': {$slice: 99}, 'p': {$slice: -10}}"), cleanString(query.toTopLevelProjection))
    }

    @Test def test_R5b() {
        println("------ test_R5b")

        var jpExpr = """$.p[0:10]"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "6"), None)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {$eq: '6'}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: 10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[:10]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, "6"), None)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {$eq: '6'}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: 10}}"), cleanString(query.toTopLevelProjection))

        println("-----------------------------------")
        jpExpr = """$.p[:10].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println("Query     : " + query.toString)
        println("Projection: " + query.toTopLevelProjection)
        assertEquals(cleanString("'p': {$elemMatch: {'q': {$exists: true, $ne: null}}}"), cleanString(query.toString))
        assertEquals(cleanString("{'p': {$slice: 10}}"), cleanString(query.toTopLevelProjection))
    }

    @Test def test_R6b() {
        println("------ test_R6b")

        var jpExpr = """$.p[0][(@.length -1)]"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(2)), None)
        println(query.toString)
        assertEquals(cleanString("$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0][this.p[0].length - 1] == 2'}]"), cleanString(query.toString))
    }

    @Test def test_R6c() {
        println("------ test_R6c")

        var jpExpr = """$.p[0][(@.length -1)].s"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(2)), None)
        println(query.toString)
        assertEquals(cleanString("$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0][this.p[0].length - 1].s == 2'}]"), cleanString(query.toString))
    }

    @Test def test_R7() {
        println("------ test_R7")

        var jpExpr = """$.p[5].*.r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertEquals(cleanString("'p.5': {$elemMatch: {'r': {$exists: true, $ne: null}}}"), cleanString(query.toString))

        jpExpr = """$.p[*][5].r"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$elemMatch: {'5.r': {$eq: 1}}}"), cleanString(query.toString))
    }

    @Test def test_R8() {
        println("------ test_R8")

        var jpExpr = """$.p[5][6].q.r"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionNotNull(jpExpr), None)
        println(query.toString)
        assertEquals(cleanString("'p.5.6.q.r': {$exists: true, $ne: null}"), cleanString(query.toString))

        jpExpr = """$.p"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$eq: 1}"), cleanString(query.toString))

        jpExpr = """$["p"]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), None)
        println(query.toString)
        assertEquals(cleanString("'p': {$eq: 1}"), cleanString(query.toString))

        jpExpr = """$.p[5].q.r"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(1)), None)
        println(query.toString)
        assertEquals(cleanString("'p.5.q.r': {$eq: 1}"), cleanString(query.toString))
    }

    @Test def test_R9() {
        println("------ test_R9")

        var jpExpr = """$.p[(@.q - @.p)].*"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(2)), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])

        jpExpr = """$.p.*[(@.q + @.p)]"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionEquals(jpExpr, new Integer(2)), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeNotSupported])
    }

    @Test def test_IsNullCondition() {
        println("------ test_IsNullCondition")

        var jpExpr = """$.p"""
        var query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionIsNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeOr])
        assertEquals(cleanString("$or: [{'p':{$exists: false}}, {'p':{$eq: null}}]"), cleanString(query.toString))

        jpExpr = """$.p[5].q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionIsNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeOr])
        assertEquals(cleanString("$or: [{'p.5.q':{$exists: false}}, {'p.5.q':{$eq: null}}]"), cleanString(query.toString))

        jpExpr = """$.p.*.q"""
        query = JsonPathToMongoTranslator.trans(new AbstractQueryConditionIsNull(jpExpr), None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeOr])
        assertEquals(cleanString("$or: [{'p':{$elemMatch: {'q': {$exists: false}}}}, {'p':{$elemMatch: {'q': {$eq: null}}}}]"), cleanString(query.toString))
    }

    @Test def test_OrEqualsIsNullCondition() {
        println("------ test_OrEqualsIsNullCondition")

        var jpExpr = """$.p"""

        val cond = AbstractQueryConditionOr.create(Set(
            new AbstractQueryConditionEquals(jpExpr, new Integer(2)),
            new AbstractQueryConditionIsNull(jpExpr)
        ))

        var query = JsonPathToMongoTranslator.trans(cond, None)
        println(query.toString)
        assertTrue(query.isInstanceOf[MongoQueryNodeOr])
        assertEquals(cleanString("$or: [{'p':{$eq: 2}}, {$or: [{'p':{$exists: false}}, {'p':{$eq: null}}]}]"), cleanString(query.toString))

        val optQ = query.optimize
        println(optQ.toString)
        assertEquals(cleanString("$or: [{'p':{$eq: 2}}, {'p':{$exists: false}}, {'p':{$eq: null}}]"), cleanString(optQ.toString))
    }
}