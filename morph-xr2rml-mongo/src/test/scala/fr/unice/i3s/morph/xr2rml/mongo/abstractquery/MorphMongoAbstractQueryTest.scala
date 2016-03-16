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
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery

class MorphMongoAbstractQueryTest {

    @Test def test_mergeAbstractAtmoicQuery_mergeok_emptyq() {
        println("------ test_mergeAbstractAtmoicQuery_mergeok_emptyq")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        // Two empty query either '' or '{}' 
        val ls1 = new xR2RMLQuery("db.collection.find()", "JSONPath", None)
        val ls1bis = new xR2RMLQuery("db.collection.find({})", "JSONPath", None)

        val proj1 = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj1bis = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))

        val cond1 = new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")
        val cond2 = new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")

        val tp1 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp2 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value2"))

        var q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm)), ls1, Set(proj1), Set(cond1))
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp2, tm)), ls1bis, Set(proj1bis), Set(cond2))

        var q = q1.mergeForInnerJoin(q2)
        println(q.get)
        assertTrue(q.isDefined)
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")))
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp1, tm)))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp2, tm)))
    }

    @Test def test_mergeAbstractAtmoicQuery_mergeok() {
        println("------ test_mergeAbstractAtmoicQuery_mergeok")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls1 = new xR2RMLQuery("db.collection.find({query})", "JSONPath", None)
        val ls1bis = new xR2RMLQuery("db.collection.find({query})", "JSONPath", None)

        val proj1 = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj1bis = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj2 = new MorphBaseQueryProjection(Set("ref2"), Some("?y"))

        val cond1 = new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")
        val cond2 = new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")

        val tp1 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp1bis = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp2 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value2"))

        var q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm), new TPBinding(tp2, tm)), ls1, Set(proj1), Set(cond1))
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls1bis, Set(proj1bis, proj2), Set(cond2))

        var q = q1.mergeForInnerJoin(q2)
        println(q.get)
        assertTrue(q.isDefined)
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")))
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp1, tm)))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp2, tm)))
    }

    @Test def test_mergeAbstractAtmoicQuery_subquery() {
        println("------ test_mergeAbstractAtmoicQuery_subquery")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls1 = new xR2RMLQuery("db.collection.find({query1})", "JSONPath", None)
        val ls2 = new xR2RMLQuery("db.collection.find({query1, query2})", "JSONPath", None)

        val proj1 = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))
        val proj1bis = new MorphBaseQueryProjection(Set("ref1"), Some("?x"))

        val cond1 = new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")
        val cond2 = new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")

        val tp1 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value"))
        val tp2 = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createURI("http://tutu"), NodeFactory.createLiteral("value2"))

        var q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm)), ls1, Set(proj1), Set(cond1))
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp2, tm)), ls2, Set(proj1bis), Set(cond2))

        var q = q1.mergeForInnerJoin(q2)
        println(q.get)
        assertTrue(q.isDefined)
        assertEquals(ls2, q.get.from)
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")))
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp1, tm)))
        assertTrue(q.get.tpBindings.contains(new TPBinding(tp2, tm)))
    }

    @Test def test_mergeAbstractAtmoicQuery_nomerge() {
        println("------ test_mergeAbstractAtmoicQuery_nomerge")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls1 = new xR2RMLQuery("db.collection.find({query1})", "JSONPath", None)
        val ls1bis = new xR2RMLQuery("db.collection.find({query2})", "JSONPath", None)
        val ls2 = new xR2RMLQuery("db.collection.find({query2})", "JSONPath", None)

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
        var q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls2, Set(proj1bis), Set(cond2))
        var q = q1.mergeForInnerJoin(q2)
        println(q)
        assertFalse(q.isDefined)

        // Same triple but not the same projection
        q1 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1, tm)), ls1, Set(proj1), Set(cond1))
        q2 = new MorphAbstractAtomicQuery(Set(new TPBinding(tp1bis, tm)), ls1bis, Set(proj2), Set(cond2))
        q = q1.mergeForInnerJoin(q2)
        println(q)
        assertFalse(q.isDefined)
    }

    @Test def test_mergeAbstractAtmoicQuery2() {
        println("------ test_mergeAbstractAtmoicQuery2")

        val tm = new R2RMLTriplesMap(null, null, null, null)
        tm.name = "tm"

        val ls = new xR2RMLQuery("db.collection.find({query}", "JSONPath", None)

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

        var q = q1.mergeForInnerJoin(q2)
        println(q.get)
        assertTrue(q.isDefined)
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond1", ConditionType.Equals, "value1")))
        assertTrue(q.get.where.contains(new MorphBaseQueryCondition("ref_cond2", ConditionType.Equals, "value2")))
        assertTrue(q.get.project.contains(proj1))
        assertTrue(q.get.project.contains(proj2))
        assertTrue(q.get.project.contains(proj3))
    }

    /**
     * Implement the optimization algorithm of an inner join query on list of Ints.
     * Instead of merging queries, we replace 2 equals elements Ints with this Int.
     */
    def optimizeQueryInnerJoinAlgo(members: List[Int]): List[Int] = {

        if (members.size == 1) { // security test but abnormal case, should never happen
            println("Unexpected case: inner join with only one member: " + this.toString)
            return members
        }

        var membersV = members
        var continue = true

        while (continue) {
            for (i: Int <- 0 to (membersV.size - 2) if continue) {
                for (j: Int <- (i + 1) to (membersV.size - 1) if continue) {

                    val left = membersV(i)
                    val right = membersV(j)

                    // Inner join of 2 atomic queries
                    if (left == right) {
                        // Note: list.slice(start, end) : start included until end excluded. slice(n,n) => empty list
                        //
                        //     i     j     =>   slice(0,i),  merged(i,j),  slice(i+1,j),  slice(j+1, size)
                        // (0, 1, 2, 3, 4) =>   0         ,  merged(1,3),  2           ,  4
                        val merged = left
                        membersV = membersV.slice(0, i) ++ List(merged) ++
                            membersV.slice(i + 1, j) ++ membersV.slice(j + 1, membersV.size)
                        continue = false
                    }
                } // end for j
            } // end for i

            if (continue) {
                // There was no more change in this run, we cannot do any optimization anymore
                return membersV
            } else
                // There was a change in this run let's rerun the optimization from with the new list of members
                continue = true
        }
        throw new MorphException("We should not quit the function this way.")
    }

    @Test def test_optimizeQueryAlgo() {
        println("------ test_optimizeQueryAlgo")

        var l = List(0, 1, 2, 3, 4)
        var lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(0, 1, 2, 3, 4), lopt)

        l = List(0, 1, 2, 1, 4)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(0, 1, 2, 4), lopt)

        l = List(0, 1, 2, 1, 4, 5, 1)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(0, 1, 2, 4, 5), lopt)

        l = List(0, 0)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(0), lopt)

        l = List(0, 1)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(0, 1), lopt)

        l = List(0, 1, 2, 3, 3, 3, 3, 3)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(0, 1, 2, 3), lopt)

        l = List(3, 3, 3, 3, 3, 4)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(3, 4), lopt)

        l = List(3, 3, 3, 3)
        lopt = optimizeQueryInnerJoinAlgo(l)
        println(lopt)
        assertEquals(List(3), lopt)
    }

}