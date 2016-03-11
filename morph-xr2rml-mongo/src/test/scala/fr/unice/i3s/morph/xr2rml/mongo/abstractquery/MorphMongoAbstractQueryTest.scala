package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProjection
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.TPBinding
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

class MorphMongoAbstractQueryTest {

    @Test def test_mergeWithAbstractAtmoicQuery() {
        println("------ test_mergeWithAbstractAtmoicQuery")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls1 = new xR2RMLQuery("query", "JSONPath", None)
        val ls1bis = new xR2RMLQuery("query", "JSONPath", None)

        val proj1 = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj1bis = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj2 = new MorphBaseQueryProjection(Set("ref1", "ref2"), Some("?x"))

        val cond1 = new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")
        val cond2 = new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")

        val tp1 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp1bis = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp2 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value2"))

        var q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm), new TPBinding(tp2, tm)), ls1, Set(proj1), Set(cond1))
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls1bis, Set(proj1bis), Set(cond2))

        var q = q1.mergeWithAbstractAtmoicQuery(q2)
        println(q.get)
        assertTrue(q.isDefined)
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")))
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp1, tm)))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp2, tm)))
    }

    @Test def test_mergeWithAbstractAtmoicQuery_nomerge() {
        println("------ test_mergeWithAbstractAtmoicQuery_nomerge")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls1 = new xR2RMLQuery("query1", "JSONPath", None)
        val ls1bis = new xR2RMLQuery("query1", "JSONPath", None)
        val ls2 = new xR2RMLQuery("query2", "JSONPath", None)

        val proj1 = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj1bis = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj2 = new MorphBaseQueryProjection(Set("ref1", "ref2"), Some("?x"))

        val cond1 = new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")
        val cond2 = new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")

        val tp1 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp1bis = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp2 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value2"))

        // Not the same logical source
        var q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm)), ls1, Set(proj1), Set(cond1))
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls2, Set(proj2), Set(cond2))
        var q = q1.mergeWithAbstractAtmoicQuery(q2)
        println(q)
        assertFalse(q.isDefined)

        // Same triple but not the same projection
        q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm)), ls1, Set(proj1), Set(cond1))
        q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls1bis, Set(proj2), Set(cond2))
        q = q1.mergeWithAbstractAtmoicQuery(q2)
        println(q)
        assertFalse(q.isDefined)
    }

    @Test def test_mergeWithAbstractAtmoicQuery2() {
        println("------ test_mergeWithAbstractAtmoicQuery2")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls = new xR2RMLQuery("query", "JSONPath", None)

        // One common projection on ?x with different order proj1=(ref1, ref2) vs. proj1bis=(ref2, ref1)
        // One other non matching projection on ?x (proj2)
        // One projection on ?y
        val proj1 = new MorphBaseQueryProjection(Set("ref1", "ref2"), Some("?x"))
        val proj1bis = new MorphBaseQueryProjection(Set("ref2", "ref1"), Some("?x"))
        val proj2 = new MorphBaseQueryProjection(Set("ref3", "ref4"), Some("?x"))
        val proj3 = new MorphBaseQueryProjection(Set("ref3", "ref4"), Some("?y"))

        val cond1 = new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")
        val cond2 = new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")

        val tp1 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp1bis = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))

        var q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm)), ls, Set(proj1, proj3), Set(cond1))
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls, Set(proj2, proj1bis), Set(cond2))

        var q = q1.mergeWithAbstractAtmoicQuery(q2)
        println(q.get)
        assertTrue(q.isDefined)
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")))
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")))
        assertTrue(q.get.project.contains(proj1))
        assertTrue(q.get.project.contains(proj2))
        assertTrue(q.get.project.contains(proj3))
    }
}