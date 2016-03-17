package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op

import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionNotNull
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class MorphBaseQueryTranslatorConcret(factory: IMorphFactory) extends MorphBaseQueryTranslator(factory) {

    override def translate(op: Op): Option[AbstractQuery] = { None }

    override def transTPm(tpBindings: TPBindings): AbstractQuery = {
        throw new MorphException("Not supported")
    }
}

class MorphBaseQueryTranslatorTest {

    val factory = new MorphFactoryConcret
    factory.properties = MorphProperties.apply("src/test/resources/query_translator", "morph.properties")
    factory.mappingDocument = R2RMLMappingDocument(factory.properties, null)

    var queryTranslator = new MorphQueryTranslatorConcret(factory)

    val tmMovies = factory.mappingDocument.getClassMappingsByName("Movies")
    val tmDirectors = factory.mappingDocument.getClassMappingsByName("Directors")
    val tmOther = factory.mappingDocument.getClassMappingsByName("Other")

    @Test def test_genProjection() {
        println("------ test_genProjection")

        // Triple pattern: ?x ex:starring "T. Leung"
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/starring")
        var o = NodeFactory.createLiteral("T. Leung")
        var tp = Triple.create(s, p, o)
        var proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(Set("$.code"), proj.head.references)
        assertEquals("?x", proj.head.as.get)

        // Triple pattern: ex:movieY ex:starring ?y
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createURI("http://example.org/starring")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(Set("$.actors.*"), proj.head.references)
        assertEquals("?y", proj.head.as.get)

        // Triple pattern: ex:movieY ?p "T. Leung" - projection of a constant term map
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createLiteral("T. Leung")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(Set("http://example.org/starring"), proj.head.references)
        assertEquals("?p", proj.head.as.get)

        // Triple pattern: ?x ?p ?y
        s = NodeFactory.createVariable("x")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmOther)
        println(proj)
        assertTrue(proj.head.references.contains("$.code"))
        assertEquals("?x", proj.head.as.get)
        assertTrue(proj.tail.head.references.contains("$.relation.prop"))
        assertEquals("?p", proj.tail.head.as.get)
        assertTrue(proj.tail.tail.head.references.contains("$.relation.actors.*"))
        assertEquals("?y", proj.tail.tail.head.as.get)
    }

    @Test def test_genProjection_RefObjectMap() {
        println("------ test_genProjection_RefObjectMap")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/directed")
        var o = NodeFactory.createURI("http://example.org/movie/Manh")
        var tp = Triple.create(s, p, o)

        var proj = queryTranslator.genProjection(tp, tmDirectors)
        println(proj)
        assertTrue(proj.head.references.contains("$.name"))
        assertEquals("?x", proj.head.as.get)
        assertFalse(proj.head.references.contains("$.dirname"))

        assertTrue(proj.tail.head.references.contains("$.directed.*"))
        assertEquals(None, proj.tail.head.as)
        assertFalse(proj.tail.head.references.contains("$.code"))
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
        assertFalse(proj.head.references.contains("$.name"))
        assertFalse(proj.head.references.contains("$.directed.*"))
        assertFalse(proj.head.references.contains("$.code"))

        assertTrue(proj.head.references.contains("$.dirname"))
        assertEquals(None, proj.head.as)
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

        assertTrue(cond.contains(new AbstractQueryConditionNotNull("$.code")))
        assertTrue(cond.contains(new AbstractQueryConditionEquals("$.actors.*", "T. Leung")))
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

        assertTrue(cond.contains(new AbstractQueryConditionEquals("$.code", "MovieY")))
        assertTrue(cond.contains(new AbstractQueryConditionNotNull("$.actors.*")))
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

        assertTrue(cond.contains(new AbstractQueryConditionEquals("$.code", "MovieY")))
        assertTrue(cond.contains(new AbstractQueryConditionEquals("$.relation.prop", "starring")))
        assertTrue(cond.contains(new AbstractQueryConditionNotNull("$.relation.actors.*")))
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

        assertTrue(cond.contains(new AbstractQueryConditionNotNull("$.dirname")))
        assertTrue(cond.contains(new AbstractQueryConditionEquals("$.code", "Manh")))
        assertFalse(cond.contains(new AbstractQueryConditionNotNull("$.directed.*")))
        assertFalse(cond.contains(new AbstractQueryConditionNotNull("$.name")))
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

        assertTrue(cond.contains(new AbstractQueryConditionNotNull("$.dirname"))) // join parent reference
        assertTrue(cond.contains(new AbstractQueryConditionNotNull("$.code"))) // parent subject
        assertFalse(cond.contains(new AbstractQueryConditionNotNull("$.directed.*")))
        assertFalse(cond.contains(new AbstractQueryConditionNotNull("$.name")))
    }

    @Test def test() {
        var s1 = NodeFactory.createVariable("x")
        var s2 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/directed")
        var p2 = NodeFactory.createURI("http://example.org/directed")
        var o1 = NodeFactory.createLiteral("lit")
        var o2 = NodeFactory.createLiteral("lit")

        println(s1 == s2)
        println(p1 == p2)
        println(o1 == o2)

        println(s1 == o2)
        println(o1 == p2)
        println(p1 == s1)
    }
}

