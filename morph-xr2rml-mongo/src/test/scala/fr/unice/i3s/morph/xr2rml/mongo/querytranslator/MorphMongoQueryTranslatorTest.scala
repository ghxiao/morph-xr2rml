package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCompare
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeElemMatch
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeUnion
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjection
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjectionArraySlice

class MorphMongoQueryTranslatorTest {

    var props = MorphProperties.apply("src/test/resources/query_translator", "morph.properties")
    var mappingDocument = R2RMLMappingDocument("query_translator/mapping.ttl", props, null)
    var queryTranslator = new MorphMongoQueryTranslator(mappingDocument)

    val tmMovies = mappingDocument.getClassMappingsByName("Movies")
    val tmDirectors = mappingDocument.getClassMappingsByName("Directors")
    val tmOther = mappingDocument.getClassMappingsByName("Other")

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_genFrom() {
        println("------ test_genFrom")

        var from = queryTranslator.genFrom(tmMovies)
        println(from)
        assertEquals(new MongoDBQuery("movies", "decade:{$exists:true}").setIterator(Some("$.movies.*")), from)

        from = queryTranslator.genFrom(tmDirectors)
        println(from)
        assertEquals(new MongoDBQuery("directors", ""), from)
    }

    @Test def test_genFromParent() {
        println("------ test_genFromParent")

        var from = queryTranslator.genFromParent(tmDirectors)
        println(from)
        assertEquals(new MongoDBQuery("movies", "decade:{$exists:true}").setIterator(Some("$.movies.*")), from)

        try {
            from = queryTranslator.genFromParent(tmMovies)
        } catch {
            case e: MorphException => {
                println("Caught exception: " + e.getMessage())
                assertTrue(true)
            }
            case e: Exception => {
                println(e.getMessage())
                Assert.fail()
            }
        }
    }

    @Test def test_genProjection() {
        println("------ test_genProjection")

        // Triple pattern: ?x ex:starring "T. Leung"
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/starring")
        var o = NodeFactory.createLiteral("T. Leung")
        var tp = Triple.create(s, p, o)
        var proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List("$.code"), proj)

        // Triple pattern: ex:movieY ex:starring ?y
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createURI("http://example.org/starring")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List("$.actors.*"), proj)

        // Triple pattern: ex:movieY ?p "T. Leung" - projection of a constant term map
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createLiteral("T. Leung")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List("http://example.org/starring"), proj)

        // Triple pattern: ?x ?p ?y
        s = NodeFactory.createVariable("x")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmOther)
        println(proj)
        assertTrue(proj.contains("$.code"))
        assertTrue(proj.contains("$.relation.prop"))
        assertTrue(proj.contains("$.relation.actors.*"))
    }

    @Test def test_genProjectionRefObjectMap() {
        println("------ test_genProjectionRefObjectMap")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/directed")
        var o = NodeFactory.createURI("http://example.org/movie/Manh")
        var tp = Triple.create(s, p, o)

        var proj = queryTranslator.genProjection(tp, tmDirectors)
        println(proj)
        assertTrue(proj.contains("$.name"))
        assertTrue(proj.contains("$.directed.*"))
        assertFalse(proj.contains("$.dirname"))
        assertFalse(proj.contains("$.code"))
    }

    @Test def test_genProjectionParent() {
        println("------ test_genProjectionParent")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/directed")
        var o = NodeFactory.createURI("http://example.org/movie/Manh")
        var tp = Triple.create(s, p, o)

        var proj = queryTranslator.genProjectionParent(tp, tmDirectors)
        println(proj)
        assertFalse(proj.contains("$.name"))
        assertFalse(proj.contains("$.directed.*"))
        assertTrue(proj.contains("$.dirname"))
        assertFalse(proj.contains("$.code"))
    }

    @Test def test_genCond_equalsLiteral() {
        println("------ test_genCond_equalsLiteral")

        // --- Triple pattern: ?x ex:starring "T. Leung"
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/starring")
        var o = NodeFactory.createLiteral("T. Leung")
        var tp = Triple.create(s, p, o)

        var cond = queryTranslator.genCond(tp, tmMovies)
        println(cond)

        assertTrue(cond.contains(new MorphBaseQueryCondition("$.code", ConditionType.IsNotNull, null)))
        assertTrue(cond.contains(new MorphBaseQueryCondition("$.actors.*", ConditionType.Equals, "T. Leung")))
    }

    @Test def test_genCond_equalsUri() {
        println("------ test_genCond_equalsUri")

        // --- Triple pattern: ex:movieY ex:starring ?y
        var s = NodeFactory.createURI("http://example.org/movie/MovieY")
        var p = NodeFactory.createURI("http://example.org/starring")
        var o = NodeFactory.createVariable("y")
        var tp = Triple.create(s, p, o)

        var cond = queryTranslator.genCond(tp, tmMovies)
        println(cond)

        assertTrue(cond.contains(new MorphBaseQueryCondition("$.code", ConditionType.Equals, "MovieY")))
        assertTrue(cond.contains(new MorphBaseQueryCondition("$.actors.*", ConditionType.IsNotNull, null)))
    }

    @Test def test_genCond_equalsUriPred() {
        println("------ test_genCond_equalsUriPred")

        // --- Triple pattern: ex:movieY ex:starring ?y
        var s = NodeFactory.createURI("http://example.org/movie/MovieY")
        var p = NodeFactory.createURI("http://example.org/property/starring")
        var o = NodeFactory.createVariable("y")
        var tp = Triple.create(s, p, o)

        var cond = queryTranslator.genCond(tp, tmOther)
        println(cond)

        assertTrue(cond.contains(new MorphBaseQueryCondition("$.code", ConditionType.Equals, "MovieY")))
        assertTrue(cond.contains(new MorphBaseQueryCondition("$.relation.prop", ConditionType.Equals, "starring")))
        assertTrue(cond.contains(new MorphBaseQueryCondition("$.relation.actors.*", ConditionType.IsNotNull, null)))
    }

    @Test def test_genCondParent_Uri() {
        println("------ test_genCondParent_Uri")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/directed")
        var o = NodeFactory.createURI("http://example.org/movie/Manh")
        var tp = Triple.create(s, p, o)

        var cond = queryTranslator.genCondParent(tp, tmDirectors)
        println(cond)

        assertTrue(cond.contains(new MorphBaseQueryCondition("$.dirname", ConditionType.IsNotNull, null)))
        assertTrue(cond.contains(new MorphBaseQueryCondition("$.code", ConditionType.Equals, "Manh")))
        assertFalse(cond.contains(new MorphBaseQueryCondition("$.directed.*", ConditionType.IsNotNull, null)))
        assertFalse(cond.contains(new MorphBaseQueryCondition("$.name", ConditionType.IsNotNull, null)))
    }

    @Test def test_genCondParent_Variable() {
        println("------ test_genCondParent_Variable")

        // Triple pattern: <http://example.org/tutu> ex:directed ?x 
        var s = NodeFactory.createURI("http://example.org/tutu")
        var p = NodeFactory.createURI("http://example.org/directed")
        var o = NodeFactory.createVariable("x")
        var tp = Triple.create(s, p, o)

        var cond = queryTranslator.genCondParent(tp, tmDirectors)
        println(cond)

        assertTrue(cond.contains(new MorphBaseQueryCondition("$.dirname", ConditionType.IsNotNull, null))) // join parent reference
        assertTrue(cond.contains(new MorphBaseQueryCondition("$.code", ConditionType.IsNotNull, null))) // parent subject
        assertFalse(cond.contains(new MorphBaseQueryCondition("$.directed.*", ConditionType.IsNotNull, null)))
        assertFalse(cond.contains(new MorphBaseQueryCondition("$.name", ConditionType.IsNotNull, null)))
    }

    @Test def test_NonNormalizedTM() {
        println("------ test_error")

        // Triple pattern: ?x ex:starring "T. Leung"
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/starring")
        val o = NodeFactory.createLiteral("T. Leung")
        val tp = Triple.create(s, p, o)

        var tm = mappingDocument.getClassMappingsByName("TM_NoPOM")
        var res = queryTranslator.transTP(tp, tm)
        assertTrue(res.queries.isEmpty)

        tm = mappingDocument.getClassMappingsByName("TM_MultiplePOM")
        res = queryTranslator.transTP(tp, tm)
        assertTrue(res.queries.isEmpty)
    }

    @Test def test_toConcreteQueries_Field() {
        println("------ test_toConcreteQueries_Field")
        val field = new MongoQueryNodeField("b", List(new MongoQueryNodeCond(ConditionType.Equals, "bbb")))
        val field2 = new MongoQueryNodeField("b.0.b", List(new MongoQueryNodeCond(ConditionType.Equals, "b0b")))

        println("---------------------------------")
        var fromPart = new MongoDBQuery("collection", "tititutu")
        var result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(field))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'b':{$eq:'bbb'}"))
    }

    @Test def test_toConcreteQueries() {
        println("------ test_toConcreteQueries")
        var fromPart = new MongoDBQuery("collection", "tititutu")

        val compare = new MongoQueryNodeField("a", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))
        val field2 = new MongoQueryNodeField("b.0.b", List(new MongoQueryNodeCond(ConditionType.Equals, "b0b")))
        val exists1 = new MongoQueryNodeField("c", new MongoQueryNodeExists)
        val exists2 = new MongoQueryNodeField("d", new MongoQueryNodeExists)
        val or = new MongoQueryNodeOr(List(exists1, exists2))
        val and2 = new MongoQueryNodeAnd(List(compare, field2))
        val union = new MongoQueryNodeUnion(List(compare, or))

        println("---------------------------------")
        // Remove top-level AND
        var result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(and2))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(0).query).contains("'b.0.b':{$eq:'b0b'}"))

        println("---------------------------------")
        result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(compare, field2))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(0).query).contains("'b.0.b':{$eq:'b0b'}"))

        println("---------------------------------")
        // Remove top-level AND from AND(OR, AND)
        result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(or, and2))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(0).query).contains("'b.0.b':{$eq:'b0b'}"))
        assertTrue(cleanString(result(0).query).contains("$or:[{'c':{$exists:true}},{'d':{$exists:true}}]"))

        println("---------------------------------")
        // Top-level UNION
        result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(union))
        println(result)
        assertTrue(result.size == 2)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(1).query).contains("tititutu"))
        assertTrue(cleanString(result(1).query).contains("$or:[{'c':{$exists:true}},{'d':{$exists:true}}]"))
    }

    @Test def test_toConcreteQueries_FusionQueries() {
        println("------ test_toConcreteQueries_FusionQueries")
        var fromPart = new MongoDBQuery("collection", "")

        val compare = new MongoQueryNodeField("a.b", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))
        val exists = new MongoQueryNodeField("a.b", new MongoQueryNodeExists)
        val field = new MongoQueryNodeField("c", List(new MongoQueryNodeCond(ConditionType.Equals, "ccc")))

        println("---------------------------------")
        var result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(compare, field, exists))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("'a.b':{$gt:10,$exists:true}"))
        assertTrue(cleanString(result(0).query).contains("'c':{$eq:'ccc'}"))

        println("---------------------------------")
        result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(compare, field, exists))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("'a.b':{$gt:10,$exists:true}"))
        assertTrue(cleanString(result(0).query).contains("'c':{$eq:'ccc'}"))

    }

    @Test def test_toConcreteQueries_FusionQueries_ElemMatch() {
        println("------ test_toConcreteQueries_FusionQueries_ElemMatch")
        var fromPart = new MongoDBQuery("collection", "")

        val elemmatch = new MongoQueryNodeField(
            "a.b",
            List(new MongoQueryNodeElemMatch(new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))),
            List(new MongoQueryProjectionArraySlice("a.b", "10")))
        val elemmatch2 = new MongoQueryNodeField(
            "a.b", new MongoQueryNodeElemMatch(new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.LT, "20")))
        val arraysize = new MongoQueryNodeField(
            "a.b", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.SIZE, "99"))

        // 2 arrays with one slice
        var result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(elemmatch, elemmatch2))
        println(result(0).toString)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("{'a.b':{$elemMatch:{$gt:10,$lt:20}}}"))
        assertTrue(cleanString(result(0).projection).contains("{'a.b':{$slice:10}}"))

        println("---------------------------------")
        // 2 arrays with one slice and one size
        result = queryTranslator.toConcreteQueries(fromPart, List.empty, List(elemmatch, elemmatch2, arraysize))
        println(result(0).toString)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("{'a.b':{$size:99,$elemMatch:{$gt:10,$lt:20}}}"))
        assertTrue(cleanString(result(0).projection).contains("{'a.b':{$slice:10}}"))
    }

    @Test def test_full() {
        println("------ test_full")

        // Triple pattern: ?x ex:starring "T. Leung"
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/starring")
        val o = NodeFactory.createLiteral("T. Leung")
        val tp = Triple.create(s, p, o)

        val conds = queryTranslator.transTP(tp, tmMovies)
        println(conds)
    }

    @Test def test_full2() {
        println("------ test_full2")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/directed")
        val o = NodeFactory.createURI("http://example.org/movie/Manh")
        val tp = Triple.create(s, p, o)

        val conds = queryTranslator.transTP(tp, tmDirectors)
        println(conds)
    }

}