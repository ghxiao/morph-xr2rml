package fr.unice.i3s.morph.xr2rml.mongo.query

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MongoQueryNodeTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test() {
        println("------------------------------------------------- test")
        val node: MongoQueryNode =
            new MongoQueryNodeField("p1",
                new MongoQueryNodeField("p2",
                    new MongoQueryNodeElemMatch(
                        new MongoQueryNodeField("p3",
                            new MongoQueryNodeElemMatch(
                                new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(3)))))))
        println(node.toString)
        assertEquals("""'p1.p2': {$elemMatch: {'p3': {$elemMatch: {$eq: 3}}}}""", node.toString)
    }

    @Test def test2() {
        println("------------------------------------------------- test2")
        val node: MongoQueryNode =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p",
                        new MongoQueryNodeField("0",
                            new MongoQueryNodeField("q",
                                new MongoQueryNodeField("1",
                                    new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))),
                    new MongoQueryNodeField("p",
                        new MongoQueryNodeField("0",
                            new MongoQueryNodeField("q",
                                new MongoQueryNodeField("3",
                                    new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))))),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p",
                        new MongoQueryNodeField("2",
                            new MongoQueryNodeField("q",
                                new MongoQueryNodeField("1",
                                    new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))),
                    new MongoQueryNodeField("p",
                        new MongoQueryNodeField("2",
                            new MongoQueryNodeField("q",
                                new MongoQueryNodeField("3",
                                    new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null)))))))
            ))
        println(node.toString)
        assertEquals("""$or: [{'p.0.q.1': {$exists: true, $ne: null}}, {'p.0.q.3': {$exists: true, $ne: null}}, {'p.2.q.1': {$exists: true, $ne: null}}, {'p.2.q.3': {$exists: true, $ne: null}}]""",
            node.optimize.toString)
    }

    @Test def test3() {
        println("------------------------------------------------- test3")
        val node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeElemMatch(
                        new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(1)))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p.0"),
                    new MongoQueryNodeWhere("this.p[0] == 0"),
                    new MongoQueryNodeCompare("p", MongoQueryNodeCompare.Operator.SIZE, "10")
                ))
            ))
        println(node.toString)
        assertEquals("""$and: [{'p': {$elemMatch: {$eq: 1}}}, {'p.0': {$exists: true}}, {$where: 'this.p[0] == 0'}, {'p': {$size: 10}}]""",
            node.optimize.toString)
    }

    @Test def test4() {
        println("------------------------------------------------- test4")
        val node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeExists("p.q"),
                new MongoQueryNodeWhere("""this.p.q[this.p.q.length - 1] == "val"""")))
        println(node.toString)
        assertEquals("""$and: [{'p.q': {$exists: true}}, {$where: 'this.p.q[this.p.q.length - 1] == "val"'}]""",
            node.toString)
    }

    @Test def testEquals() {
        println("------------------------------------------------- testEquals")
        var node1: MongoQueryNode = null
        var node2: MongoQueryNode = null

        node1 = new MongoQueryNodeAnd(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeAnd(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == b")))
        assertTrue(node1 == node2)

        node1 = new MongoQueryNodeAnd(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeAnd(List(new MongoQueryNodeExists("q"), new MongoQueryNodeWhere("a == b")))
        assertFalse(node1 == node2)

        node1 = new MongoQueryNodeOr(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeOr(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == c")))
        assertFalse(node1 == node2)

        node1 = new MongoQueryNodeOr(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeOr(List(new MongoQueryNodeExists("q"), new MongoQueryNodeWhere("a == b")))
        assertFalse(node1 == node2)

        node1 = new MongoQueryNodeOr(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == b")))
        node2 = new MongoQueryNodeOr(List(new MongoQueryNodeExists("p"), new MongoQueryNodeWhere("a == c")))
        assertFalse(node1 == node2)

        assertTrue(new MongoQueryNodeExists("p") == new MongoQueryNodeExists("p"))
        assertFalse(new MongoQueryNodeExists("p") == new MongoQueryNodeExists("q"))

        assertTrue(new MongoQueryNodeNotExists("p") == new MongoQueryNodeNotExists("p"))
        assertFalse(new MongoQueryNodeNotExists("p") == new MongoQueryNodeNotExists("q"))

        assertTrue(new MongoQueryNodeElemMatch(new MongoQueryNodeExists("p")) == new MongoQueryNodeElemMatch(new MongoQueryNodeExists("p")))
        assertFalse(new MongoQueryNodeElemMatch(new MongoQueryNodeExists("p")) == new MongoQueryNodeElemMatch(new MongoQueryNodeExists("q")))

        assertTrue(new MongoQueryNodeField("p", new MongoQueryNodeExists("q")) == new MongoQueryNodeField("p", new MongoQueryNodeExists("q")))
        assertFalse(new MongoQueryNodeField("p", new MongoQueryNodeExists("q")) == new MongoQueryNodeField("r", new MongoQueryNodeExists("q")))
        assertFalse(new MongoQueryNodeField("p", new MongoQueryNodeExists("q")) == new MongoQueryNodeField("p", new MongoQueryNodeExists("r")))

        assertTrue(new MongoQueryNodeNotSupported("p") == new MongoQueryNodeNotSupported("p"))
        assertFalse(new MongoQueryNodeNotSupported("p") == new MongoQueryNodeNotSupported("q"))

        assertTrue(new MongoQueryNodeWhere("p") == new MongoQueryNodeWhere("p"))
        assertFalse(new MongoQueryNodeWhere("p") == new MongoQueryNodeWhere("q"))

        node1 = new MongoQueryNodeOr(List(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("0",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("1",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))),
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("0",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("3",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))))),
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("2",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("1",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))),
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("2",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("3",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null)))))))
        ))
        node1 = new MongoQueryNodeOr(List(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("0",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("1",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))),
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("0",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("3",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))))),
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("2",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("1",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null))))),
                new MongoQueryNodeField("p",
                    new MongoQueryNodeField("2",
                        new MongoQueryNodeField("q",
                            new MongoQueryNodeField("4",
                                new MongoQueryNodeCond(MongoQueryNode.CondType.IsNotNull, null)))))))
        ))
        assertTrue(node1 == node1)
        assertFalse(node1 == node2)
    }
}