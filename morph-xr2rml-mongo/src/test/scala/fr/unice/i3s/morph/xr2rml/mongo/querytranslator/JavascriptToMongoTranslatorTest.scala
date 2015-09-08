package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode

class JavascriptToMongoTranslatorTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_j0() {
        println("------------------------------------------------- test_j0 ")

        var jsExpr: String = null
        var mq: MongoQueryNode = null
        jsExpr = """this.p && this.q[1] && ! this.q1.q2""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)

        assertTrue(cleanString(mq.toQueryString).contains("$and:["))
        assertTrue(cleanString(mq.toQueryString).contains("{'q.1':{$exists:true}}"))
        assertTrue(cleanString(mq.toQueryString).contains("{'q1.q2':{$exists:false}}"))
        assertTrue(cleanString(mq.toQueryString).contains("{'p':{$exists:true}}"))
    }

    @Test def test_j1() {
        println("------------------------------------------------- test_j1 ")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """(this.s == "thy") || ("ujy" == "ujy")""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertTrue(
            cleanString(""" $or: [{'s': {$eq: 'thy'}}, {$where: '"ujy" == "ujy"'}]""").equals(cleanString(mq.toQueryString)) ||
                cleanString(""" $or: [{$where: '"ujy" == "ujy"'}, {'s': {$eq: 'thy'}}]""").equals(cleanString(mq.toQueryString)))

        jsExpr = """this.p[0].r || this.p[0].s""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertTrue(
            cleanString(""" $or: [{'p.0.r': {$exists: true}}, {'p.0.s': {$exists: true}}]""").equals(cleanString(mq.toQueryString)) ||
                cleanString(""" $or: [{'p.0.s': {$exists: true}}, {'p.0.r': {$exists: true}}]""").equals(cleanString(mq.toQueryString)))
    }

    @Test def test_j2() {
        println("------------------------------------------------- test_j2")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """this.s <= this.p[1] """;
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertTrue(cleanString(mq.toQueryString).contains("$and:["))
        assertTrue(cleanString(mq.toQueryString).contains("""{$where:'this.s<=this.p[1]'}"""))
        assertTrue(cleanString(mq.toQueryString).contains("""{'s':{$exists:true}}"""))
        assertTrue(cleanString(mq.toQueryString).contains("""{'p.1':{$exists:true}}"""))

        jsExpr = """this.s === this.p[1] """;
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertTrue(cleanString(mq.toQueryString).contains("$and:["))
        assertTrue(cleanString(mq.toQueryString).contains("""{$where:'this.s===this.p[1]'}"""))
        assertTrue(cleanString(mq.toQueryString).contains("""{'s':{$exists:true}}"""))
        assertTrue(cleanString(mq.toQueryString).contains("""{'p.1':{$exists:true}}"""))
    }

    @Test def test_j3() {
        println("------------------------------------------------- test_j3")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """this.r""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("'r': {$exists: true}"), cleanString(mq.toQueryString))
    }

    @Test def test_j4() {
        println("------------------------------------------------- test_j4")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """!this.r[0]""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("'r.0': {$exists: false}"), cleanString(mq.toQueryString))
    }

    @Test def test_j5() {
        println("------------------------------------------------- test_j5")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """this.p.length == 10""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("'p': {$size: 10}"), cleanString(mq.toQueryString))

        jsExpr = """this.p.length > 10""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("$where: 'this.p.length > 10'"), cleanString(mq.toQueryString))
    }

    @Test def test_j6() {
        println("------------------------------------------------- test_j6")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """this.r == "xyz"""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("'r': {$eq: 'xyz'}"), cleanString(mq.toQueryString))

        jsExpr = """10 == this.r""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("'r': {$eq: 10}"), cleanString(mq.toQueryString))

        jsExpr = """this.r == false""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString("'r': {$eq: false}"), cleanString(mq.toQueryString))
    }

    @Test def test_j7() {
        println("------------------------------------------------- test_j7")

        var jsExpr: String = null
        var mq: MongoQueryNode = null

        jsExpr = """this.s + this.p[1] == 20""";
        mq = JavascriptToMongoTranslator.transJS(jsExpr).getOrElse(null)
        println(mq.toQueryString)
        assertEquals(cleanString(""" $where: 'this.s + this.p[1] == 20' """), cleanString(mq.toQueryString))
    }
}
