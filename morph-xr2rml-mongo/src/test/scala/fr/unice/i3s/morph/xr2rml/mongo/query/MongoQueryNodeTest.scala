package fr.unice.i3s.morph.xr2rml.mongo.query

import org.junit.Assert.assertEquals
import org.junit.Test

class MongoQueryNodeTest {

    @Test def test() {

        var node: MongoQueryNode = null

        node =
            new MongoQueryNodeField("p1",
                new MongoQueryNodeField("p2",
                    new MongoQueryNodeElemMatch(
                        new MongoQueryNodeField("p3",
                            new MongoQueryNodeElemMatch(
                                new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(3)))))))
        println(node.toQueryString)
        assertEquals("""'p1.p2': {$elemMatch: {'p3': {$elemMatch: {$eq: 3}}}}""", node.toQueryString)

        // -------------------------------------------------------------
        node =
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
        println(node.toQueryString)
        assertEquals("""$or: [{$or: [{'p.0.q.1': {$exists: true, $ne: null}}, {'p.0.q.3': {$exists: true, $ne: null}}]}, {$or: [{'p.2.q.1': {$exists: true, $ne: null}}, {'p.2.q.3': {$exists: true, $ne: null}}]}]""",
            node.toQueryString)

        // -------------------------------------------------------------
        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p",
                    new MongoQueryNodeElemMatch(
                        new MongoQueryNodeCond(MongoQueryNode.CondType.Equals, new Integer(1)))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p.0"),
                    new MongoQueryNodeWhere("this.p[0] == 0"),
                    new MongoQueryNodeCompare("p", MongoQueryNodeCompare.Operator.SIZE, new Integer(10))
                ))
            ))
        println(node.toQueryString)
        assertEquals("""$and: [{'p': {$elemMatch: {$eq: 1}}}, {$and: [{'p.0': {$exists: true}}, {$where: 'this.p[0] == 0'}, {'p': {$size: 10}}]}]""",
            node.toQueryString)

        // -------------------------------------------------------------
        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeExists("p.q"),
                new MongoQueryNodeWhere("""this.p.q[this.p.q.length - 1] == "val"""")))
        println(node.toQueryString)
        assertEquals("""$and: [{'p.q': {$exists: true}}, {$where: 'this.p.q[this.p.q.length - 1] == "val"'}]""",
            node.toQueryString)

        // -------------------------------------------------------------
    }
}