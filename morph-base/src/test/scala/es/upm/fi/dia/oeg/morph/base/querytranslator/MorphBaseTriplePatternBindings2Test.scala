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

class MorphBaseTriplePatternBindings2Concret extends MorphBaseQueryTranslator {

    this.properties = MorphProperties.apply("src/test/resources/query_translator", "morph_test_bindings.properties")
    this.mappingDocument = R2RMLMappingDocument("query_translator/mapping_test_bindings.ttl", properties, null)

    override def translate(op: Op): MorphAbstractQuery = { null }

    override def transTPm(tp: Triple, tmSet: List[R2RMLTriplesMap]): MorphAbstractQuery = {
        throw new MorphException("Not supported")
    }
}

class MorphBaseTriplePatternBindings2Test {

    var queryTranslator = new MorphBaseTriplePatternBindings2Concret()

    MorphBaseTriplePatternBindings.mappingDocument = queryTranslator.mappingDocument

    val TM1 = queryTranslator.mappingDocument.getClassMappingsByName("TM1")
    val TM2 = queryTranslator.mappingDocument.getClassMappingsByName("TM2")
    val TM3 = queryTranslator.mappingDocument.getClassMappingsByName("TM3")
    val TM4 = queryTranslator.mappingDocument.getClassMappingsByName("TM4")

    @Test def test_join() {
        println("------ test_join")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/starring")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("x")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("z")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM1), tp2, List(TM2, TM3))
        assertTrue(compat.contains((TM1, TM2)))
        assertFalse(compat.contains((TM1, TM3))) // subject template strings are incompatible
    }

    @Test def test_join2() {
        println("------ test_join2")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/starring")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createURI("http://example.org/turlu")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("y")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM1, TM2), tp2, List(TM2, TM3))
        assertTrue(compat.contains((TM1, TM3)))
        assertTrue(compat.contains((TM2, TM2)))
        assertFalse(compat.contains((TM1, TM2))) // datatype in object map of TM2 and not in TM1 
    }

    @Test def test_join3() {
        println("------ test_join3")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/starring")
        var o1 = NodeFactory.createVariable("x")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("x")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("y")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM1, TM2), tp2, List(TM2, TM3))
        assertTrue(compat.isEmpty)
    }

    @Test def test_join4() {
        // With a RefObjectMap
        println("------ test_join4")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/directed")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("y")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("z")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM4), tp2, List(TM2, TM3))
        assertTrue(compat.contains((TM4, TM2)))
        assertFalse(compat.contains((TM4, TM3)))
    }
}