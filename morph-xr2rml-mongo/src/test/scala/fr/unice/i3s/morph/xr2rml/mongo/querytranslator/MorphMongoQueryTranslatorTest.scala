package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.TargetQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryConditionJoin

class MorphMongoQueryTranslatorTest {

    var props = MorphProperties.apply("src/test/resources/query_translator", "morph.properties")
    var mappingDocument = R2RMLMappingDocument("query_translator/mapping.ttl", props, null)
    var queryTranslator = new MorphMongoQueryTranslator(mappingDocument)

    val tmMovies = mappingDocument.getClassMappingsByName("Movies")
    val tmDirectors = mappingDocument.getClassMappingsByName("Directors")
    val tmOther = mappingDocument.getClassMappingsByName("Other")

    @Test def test_genProjection() {
        println("------ test_genProjection")

        // Triple pattern: ?x ex:starring "T. Leung"
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/starring")
        var o = NodeFactory.createLiteral("T. Leung")
        var tp = Triple.create(s, p, o)

        var proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List((TargetQuery.Child, "$.code")), proj)

        // Triple pattern: ex:movieY ex:starring ?y
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createURI("http://example.org/starring")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)

        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List((TargetQuery.Child, "$.actors.*")), proj)

        // Triple pattern: ?x ?p ?y
        s = NodeFactory.createVariable("x")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)

        proj = queryTranslator.genProjection(tp, tmOther)
        println(proj)
        assertTrue(proj.contains((TargetQuery.Child, "$.code")))
        assertTrue(proj.contains((TargetQuery.Child, "$.relation.prop")))
        assertTrue(proj.contains((TargetQuery.Child, "$.relation.actors.*")))
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
        assertTrue(proj.contains((TargetQuery.Child, "$.name")))
        assertTrue(proj.contains((TargetQuery.Child, "$.directed.*")))
        assertTrue(proj.contains((TargetQuery.Parent, "$.name")))
    }

    @Test def test_genFrom() {
        println("------ test_genFrom")

        var from = queryTranslator.genFrom(tmMovies)
        println(from)
        assertEquals(new MongoDBQuery("movies", "decade:{$exists:true}").setIterator(Some("$.movies.*")), from(TargetQuery.Child))
        assertFalse(from.contains(TargetQuery.Parent))

        from = queryTranslator.genFrom(tmDirectors)
        println(from)
        assertEquals(new MongoDBQuery("directors", ""), from(TargetQuery.Child))
        assertEquals(new MongoDBQuery("movies", "decade:{$exists:true}").setIterator(Some("$.movies.*")), from(TargetQuery.Parent))
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

        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.code", TargetQuery.Child, ConditionType.IsNotNull, null)))
        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.actors.*", TargetQuery.Child, ConditionType.Equals, "T. Leung")))
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

        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.code", TargetQuery.Child, ConditionType.Equals, "MovieY")))
        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.actors.*", TargetQuery.Child, ConditionType.IsNotNull, null)))
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

        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.code", TargetQuery.Child, ConditionType.Equals, "MovieY")))
        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.relation.prop", TargetQuery.Child, ConditionType.Equals, "starring")))
        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.relation.actors.*", TargetQuery.Child, ConditionType.IsNotNull, null)))
    }

    @Test def test_genCond_RefObjectMap() {
        println("------ test_genCond_RefObjectMap")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/directed")
        val o = NodeFactory.createURI("http://example.org/movie/Manh")
        val tp = Triple.create(s, p, o)

        var cond = queryTranslator.genCond(tp, tmDirectors)
        println(cond)

        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.name", TargetQuery.Child, ConditionType.IsNotNull, null)))
        assertTrue(cond.contains(new MorphBaseQueryCondition(
            "$.code", TargetQuery.Parent, ConditionType.Equals, "Manh")))
        assertTrue(cond.contains(new MorphBaseQueryConditionJoin(
            "$.directed.*", None, "$.name", Some("$.movies.*"))))
    }

    @Test def test_error() {
        println("------ test_error")

        // Triple pattern: ?x ex:starring "T. Leung"
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/starring")
        val o = NodeFactory.createLiteral("T. Leung")
        val tp = Triple.create(s, p, o)

        var tm = mappingDocument.getClassMappingsByName("TM_NoPOM")
        var res = queryTranslator.transTP(tp, tm)
        assertTrue(res.childQueries.isEmpty)

        tm = mappingDocument.getClassMappingsByName("TM_MultiplePOM")
        res = queryTranslator.transTP(tp, tm)
        assertTrue(res.childQueries.isEmpty)
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