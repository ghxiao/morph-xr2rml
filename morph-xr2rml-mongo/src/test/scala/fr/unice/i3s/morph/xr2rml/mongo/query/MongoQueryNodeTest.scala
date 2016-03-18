package fr.unice.i3s.morph.xr2rml.mongo.query

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

import es.upm.fi.dia.oeg.morph.base.query.ConditionType

class MongoQueryNodeTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test() {
        println("------------------------------------------------- test")
        val node: MongoQueryNode =
            new MongoQueryNodeField("p1.p2",
                new MongoQueryNodeElemMatch(
                    new MongoQueryNodeField("p3",
                        new MongoQueryNodeElemMatch(
                            new MongoQueryNodeCond(ConditionType.Equals, new Integer(3))))))
        println(node.toString)
        assertEquals(
            """'p1.p2': {$elemMatch: {'p3': {$elemMatch: {$eq: 3}}}}""",
            node.toString)
    }

    @Test def test2() {
        println("------------------------------------------------- test2")
        val node: MongoQueryNode =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p.0.q.1",
                        new MongoQueryNodeCond(ConditionType.IsNotNull, null)),
                    new MongoQueryNodeField("p.0.q.3",
                        new MongoQueryNodeCond(ConditionType.IsNotNull, null)))),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p.2.q.1",
                        new MongoQueryNodeCond(ConditionType.IsNotNull, null)),
                    new MongoQueryNodeField("p.2.q.3",
                        new MongoQueryNodeCond(ConditionType.IsNotNull, null))))
            ))
        println(node.toString)
        assertEquals(
            """$or: [{'p.0.q.1': {$exists: true, $ne: null}}, {'p.0.q.3': {$exists: true, $ne: null}}, {'p.2.q.1': {$exists: true, $ne: null}}, {'p.2.q.3': {$exists: true, $ne: null}}]""",
            node.optimize.toString)
    }

    @Test def test3() {
        println("------------------------------------------------- test3")
        val node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeElemMatch(
                        new MongoQueryNodeCond(ConditionType.Equals, new Integer(1)))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p.0", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("this.p[0] == 0"),
                    new MongoQueryNodeField("p", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.SIZE, "10"))
                ))
            ))
        println(node.toString)
        assertEquals(
            """$and: [{'p': {$elemMatch: {$eq: 1}}}, {'p.0': {$exists: true}}, {$where: 'this.p[0] == 0'}, {'p': {$size: 10}}]""",
            node.optimize.toString)
    }

    @Test def test4() {
        println("------------------------------------------------- test4")
        val node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p.q", new MongoQueryNodeCondExists),
                new MongoQueryNodeWhere("""this.p.q[this.p.q.length - 1] == "val"""")))
        println(node.toString)
        assertEquals(
            """$and: [{'p.q': {$exists: true}}, {$where: 'this.p.q[this.p.q.length - 1] == "val"'}]""",
            node.toString)
    }

    @Test def testEquals() {
        println("------------------------------------------------- testEquals")
        var node1: MongoQueryNode = null
        var node2: MongoQueryNode = null

        node1 = new MongoQueryNodeAnd(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeAnd(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        assertTrue(node1 == node2)

        node1 = new MongoQueryNodeAnd(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeAnd(List(new MongoQueryNodeField("q", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        assertFalse(node1 == node2)

        node1 = new MongoQueryNodeOr(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeOr(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == c")))
        assertFalse(node1 == node2)

        node1 = new MongoQueryNodeOr(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeOr(List(new MongoQueryNodeField("q", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        assertFalse(node1 == node2)

        node1 = new MongoQueryNodeOr(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeOr(List(new MongoQueryNodeField("p", new MongoQueryNodeCondExists), new MongoQueryNodeWhere("a == c")))
        assertFalse(node1 == node2)

        assertTrue(new MongoQueryNodeField("p", new MongoQueryNodeCondExists) == new MongoQueryNodeField("p", new MongoQueryNodeCondExists))
        assertFalse(new MongoQueryNodeField("p", new MongoQueryNodeCondExists) == new MongoQueryNodeField("q", new MongoQueryNodeCondExists))

        assertTrue(new MongoQueryNodeField("p", new MongoQueryNodeCondNotExists) == new MongoQueryNodeField("p", new MongoQueryNodeCondNotExists))
        assertFalse(new MongoQueryNodeField("p", new MongoQueryNodeCondNotExists) == new MongoQueryNodeField("q", new MongoQueryNodeCondNotExists))

        assertTrue(new MongoQueryNodeElemMatch(new MongoQueryNodeField("p", new MongoQueryNodeCondExists)) == new MongoQueryNodeElemMatch(new MongoQueryNodeField("p", new MongoQueryNodeCondExists)))
        assertFalse(new MongoQueryNodeElemMatch(new MongoQueryNodeField("p", new MongoQueryNodeCondExists)) == new MongoQueryNodeElemMatch(new MongoQueryNodeField("q", new MongoQueryNodeCondExists)))

        assertTrue(new MongoQueryNodeNotSupported("p") == new MongoQueryNodeNotSupported("p"))
        assertFalse(new MongoQueryNodeNotSupported("p") == new MongoQueryNodeNotSupported("q"))

        assertTrue(new MongoQueryNodeWhere("p") == new MongoQueryNodeWhere("p"))
        assertFalse(new MongoQueryNodeWhere("p") == new MongoQueryNodeWhere("q"))

        node1 = new MongoQueryNodeOr(List(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p.0.q.1",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null)),
                new MongoQueryNodeField("p.0.q.3",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null)))),
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p.2.q.1",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null)),
                new MongoQueryNodeField("p.2.q.3",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null))))
        ))
        node1 = new MongoQueryNodeOr(List(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p.0.q.1",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null)),
                new MongoQueryNodeField("p.0.q.3",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null)))),
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p.2.q.1",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null)),
                new MongoQueryNodeField("p.2.q.3",
                    new MongoQueryNodeCond(ConditionType.IsNotNull, null))))
        ))
        assertTrue(node1 == node1)
        assertFalse(node1 == node2)
    }

    @Test def test_fusrionQueries() {
        println("------------------------------------------------- test_fusionQueries")

        // only one field node
        val node: MongoQueryNode = new MongoQueryNodeField("a", new MongoQueryNodeCond(ConditionType.IsNotNull, null))
        var res = MongoQueryNode.fusionQueries(List(node))
        println(res)
        assertEquals(res.size, 1)
        assertEquals(cleanString(res(0).toString), cleanString("'a': {$exists: true, $ne: null}"))

        // Two field nodes with same path
        println("------------------------------")
        val node2: MongoQueryNode = new MongoQueryNodeField("a", new MongoQueryNodeCond(ConditionType.Equals, new Integer(10)))
        res = MongoQueryNode.fusionQueries(List(node, node2))
        println(res)
        assertEquals(res.size, 1)
        assertEquals(cleanString(res(0).toString), cleanString("'a': {$exists: true, $ne: null, $eq: 10}"))

        // One where node and no field node
        println("------------------------------")
        val nodewhere: MongoQueryNode = new MongoQueryNodeWhere("this.p[0] == 0")
        res = MongoQueryNode.fusionQueries(List(nodewhere))
        println(res)
        assertEquals(res.size, 1)
        assertEquals(cleanString(res(0).toString), cleanString("$where: 'this.p[0] == 0'"))

        // Mixed field nodes with 2 different paths
        println("------------------------------")
        val node3: MongoQueryNode = new MongoQueryNodeField("a", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "1"))
        val node4: MongoQueryNode = new MongoQueryNodeField("B", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))
        val node5: MongoQueryNode = new MongoQueryNodeField("B", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.LTE, "20"))
        res = MongoQueryNode.fusionQueries(List(node, node4, node2, node5, node3))
        println(res)
        assertEquals(res.size, 2)
        assertEquals(cleanString(res(0).toString), cleanString("'B': {$gt:10, $lte:20}"))
        assertEquals(cleanString(res(1).toString), cleanString("'a': {$exists: true, $ne: null, $eq: 10, $gt: 1}"))
    }

    @Test def test_fusionQueries_ElemMatch() {
        println("------------------------------------------------- test_fusionQueries_ElemMatch")

        // Only one elemmatch node
        val node: MongoQueryNode =
            new MongoQueryNodeField("a.b",
                new MongoQueryNodeElemMatch(
                    new MongoQueryNodeCond(ConditionType.Equals, "aaaa")))

        var res = MongoQueryNode.fusionQueries(List(node))
        println(res)
        assertEquals(res.size, 1)
        assertEquals(cleanString(res(0).toString), cleanString("'a.b': {$elemMatch: {$eq: 'aaaa'}}"))

        // Two elemmatch nodes with same path
        println("------------------------------")
        var node2: MongoQueryNode =
            new MongoQueryNodeField("a.b",
                new MongoQueryNodeElemMatch(
                    new MongoQueryNodeCond(ConditionType.Equals, "bbbb")))
        res = MongoQueryNode.fusionQueries(List(node, node2))
        println(res)
        assertEquals(res.size, 1)
        assertEquals(cleanString(res(0).toString), cleanString("'a.b': {$elemMatch: {$eq:'aaaa', $eq:'bbbb'}}"))

        // 3 mixed elemmatch nodes with two different same paths
        println("------------------------------")
        node2 = new MongoQueryNodeField(
            "a.b",
            List(new MongoQueryNodeElemMatch(
                new MongoQueryNodeCond(ConditionType.Equals, "bbbb")),
                new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.SIZE, "10")))

        val node3: MongoQueryNode = new MongoQueryNodeField("a.c", new MongoQueryNodeCond(ConditionType.Equals, new Integer(10)))
        res = MongoQueryNode.fusionQueries(List(node, node3, node2))
        println(res)
        assertEquals(res.size, 2)
        assertEquals(cleanString(res(0).toString), cleanString("'a.c': {$eq: 10}"))
        assertEquals(cleanString(res(1).toString), cleanString("'a.b': {$size: 10, $elemMatch: {$eq:'aaaa', $eq:'bbbb'}}"))
    }

    @Test def test_fusionQueries_ElemMatch_Projection() {
        println("------------------------------------------------- test_fusionQueries_ElemMatch_Projection")

        val node: MongoQueryNode =
            new MongoQueryNodeField(
                "a.b",
                List(new MongoQueryNodeElemMatch(
                    new MongoQueryNodeCond(ConditionType.Equals, "aaaa"))),
                List(new MongoQueryProjectionArraySlice("a.b", "-10")))

        var node2: MongoQueryNode =
            new MongoQueryNodeField("a.b",
                List(new MongoQueryNodeElemMatch(
                    new MongoQueryNodeCond(ConditionType.Equals, "bbbb"))),
                List(new MongoQueryProjectionArraySlice("a.b", "-10")))

        var res = MongoQueryNode.fusionQueries(List(node, node2))
        println(res(0))
        println(res(0).toTopLevelProjection)
        assertEquals(res.size, 1)
       
        assertEquals(cleanString(res(0).toString), cleanString("'a.b': {$elemMatch: {$eq: 'aaaa', $eq: 'bbbb'}}"))
        // This example is a bit stupid since the projection string is invalid, it has 2 times the same field 'a.b'
        // but so far the only projection operator is the array $slice. Later on we may need for than this,
        // in that case merging several projections should be necessary. For now it is probably not. So
        // this is just to test that the merging works.
        assertEquals(cleanString(res(0).toTopLevelProjection), cleanString("{'a.b': {$slice: -10}, 'a.b': {$slice: -10}}"))
    }
}