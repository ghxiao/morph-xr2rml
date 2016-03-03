package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery

class MorphBaseQueryTranslatorConcret extends MorphBaseQueryTranslator {

    this.properties = MorphProperties.apply("src/test/resources/query_translator", "morph.properties")
    this.mappingDocument = R2RMLMappingDocument("query_translator/mapping.ttl", properties, null)

    override def translate(op: Op): Option[MorphAbstractQuery] = { None }

    override def transTPm(tp: Triple, tmSet: List[R2RMLTriplesMap]): MorphAbstractQuery = {
        throw new MorphException("Not supported")
    }
}

class MorphBaseQueryTranslatorTest {

    var queryTranslator = new MorphBaseQueryTranslatorConcret()

    val tmMovies = queryTranslator.mappingDocument.getClassMappingsByName("Movies")
    val tmDirectors = queryTranslator.mappingDocument.getClassMappingsByName("Directors")
    val tmOther = queryTranslator.mappingDocument.getClassMappingsByName("Other")

    @Test def test_genProjection() {
        println("------ test_genProjection")

        // Triple pattern: ?x ex:starring "T. Leung"
        var s = NodeFactory.createVariable("x")
        var p = NodeFactory.createURI("http://example.org/starring")
        var o = NodeFactory.createLiteral("T. Leung")
        var tp = Triple.create(s, p, o)
        var proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List("$.code"), proj(0).references)
        assertEquals("?x", proj(0).as.get)

        // Triple pattern: ex:movieY ex:starring ?y
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createURI("http://example.org/starring")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List("$.actors.*"), proj(0).references)
        assertEquals("?y", proj(0).as.get)

        // Triple pattern: ex:movieY ?p "T. Leung" - projection of a constant term map
        s = NodeFactory.createURI("http://example.org/movieY")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createLiteral("T. Leung")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmMovies)
        println(proj)
        assertEquals(List("http://example.org/starring"), proj(0).references)
        assertEquals("?p", proj(0).as.get)

        // Triple pattern: ?x ?p ?y
        s = NodeFactory.createVariable("x")
        p = NodeFactory.createVariable("p")
        o = NodeFactory.createVariable("y")
        tp = Triple.create(s, p, o)
        proj = queryTranslator.genProjection(tp, tmOther)
        println(proj)
        assertTrue(proj(0).references.contains("$.code"))
        assertEquals("?x", proj(0).as.get)
        assertTrue(proj(1).references.contains("$.relation.prop"))
        assertEquals("?p", proj(1).as.get)
        assertTrue(proj(2).references.contains("$.relation.actors.*"))
        assertEquals("?y", proj(2).as.get)
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
        assertTrue(proj(0).references.contains("$.name"))
        assertEquals("?x", proj(0).as.get)
        assertFalse(proj(0).references.contains("$.dirname"))

        assertTrue(proj(1).references.contains("$.directed.*"))
        assertEquals(None, proj(1).as)
        assertFalse(proj(1).references.contains("$.code"))
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
        assertFalse(proj(0).references.contains("$.name"))
        assertFalse(proj(0).references.contains("$.directed.*"))
        assertFalse(proj(0).references.contains("$.code"))

        assertTrue(proj(0).references.contains("$.dirname"))
        assertEquals(None, proj(0).as)
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

