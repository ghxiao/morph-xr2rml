package fr.unice.i3s.morph.xr2rml.mongo.query

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MongoQueryNodeOptimizeTest {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def testOptimizeFlattenAnds() {
        println("------------------------------------------------- testOptimizeFlattenAnds")
        val node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeExists)))))
        println(node.toString)
        // Raw
        assertEquals(cleanString("""$and: [{'p': {$exists: true}}, {$and: [{'q': {$exists: true}}, {'r': {$exists: true}}]}]"""), cleanString(node.toString))
        // Optimized
        assertEquals(cleanString("""$and: [{'p': {$exists: true}}, {'q': {$exists: true}}, {'r': {$exists: true}}]"""), cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizeFlattenOrs() {
        println("------------------------------------------------- testOptimizeFlattenOrs")
        val node: MongoQueryNode =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeExists)))))
        println(node.toString)
        // Raw
        assertEquals(cleanString("""$or: [{'p': {$exists: true}}, {$or: [{'q': {$exists: true}}, {'r': {$exists: true}}]}]"""), cleanString(node.toString))
        // Optimized
        assertEquals(cleanString("""$or: [{'p': {$exists: true}}, {'q': {$exists: true}}, {'r': {$exists: true}}]"""), cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizeFlattenUnions() {
        println("------------------------------------------------- testOptimizeFlattenUnions")
        val node: MongoQueryNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeUnion(
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeExists)))
        println(node.toString)
        // Raw
        assertEquals(cleanString("""UNION: [{'p': {$exists: true}}, {UNION: [{'q': {$exists: true}}, {'r': {$exists: true}}]}]"""), cleanString(node.toString))
        // Optimized
        assertEquals(cleanString("""UNION: [{'p': {$exists: true}}, {'q': {$exists: true}}, {'r': {$exists: true}}]"""), cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizeGroupWheres() {
        println("------------------------------------------------- testOptimizeGroupWheres")
        var node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeWhere("a == b"),
                new MongoQueryNodeWhere("@.q < @.p")))
        println(node.toString)
        // Raw
        assertEquals(cleanString("""$and: [{'p': {$exists: true}}, {$where: 'a == b'}, {$where: '@.q < @.p'}]"""), cleanString(node.toString))
        // Optimized
        assertEquals(cleanString("""$and: [{'p': {$exists: true}}, {$where: '(a == b) && (@.q < @.p)'}]"""), cleanString(node.optimizeQuery.toString))

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeWhere("a == b"),
                new MongoQueryNodeWhere("@.q < @.p")))
        println(node.toString)
        // Raw
        assertEquals(cleanString("""$or: [{'p': {$exists: true}}, {$where: 'a == b'}, {$where: '@.q < @.p'}]"""), cleanString(node.toString))
        // Optimized
        assertEquals(cleanString("""$or: [{'p': {$exists: true}}, {$where: '(a == b) || (@.q < @.p)'}]"""), cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizePullupWheresW1() {
        println("------------------------------------------------- testOptimizePullupWheresW1")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeField("q", new MongoQueryNodeExists),
                new MongoQueryNodeWhere("@.p == 1")))
        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists))),
                new MongoQueryNodeWhere("@.p == 1"))
        assertEquals(optimizedNode, node.optimize)

        node = new MongoQueryNodeOr(List(
            new MongoQueryNodeField("p", new MongoQueryNodeExists),
            new MongoQueryNodeField("q", new MongoQueryNodeExists),
            new MongoQueryNodeWhere("@.p == 1"),
            new MongoQueryNodeWhere("@.q > 2")))
        optimizedNode = new MongoQueryNodeUnion(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeField("q", new MongoQueryNodeExists))),
            new MongoQueryNodeWhere("(@.p == 1) || (@.q > 2)"))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW2() {
        println("------------------------------------------------- testOptimizePullupWheresW2")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeField("q", new MongoQueryNodeExists),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("@.p == 1")))))

        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("@.p == 1"))))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW4() {
        println("------------------------------------------------- testOptimizePullupWheresW4")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeField("q", new MongoQueryNodeExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("@.p == 1")))))

        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("r", new MongoQueryNodeExists),
                        new MongoQueryNodeField("s", new MongoQueryNodeExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("@.p == 1"))))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW5() {
        println("------------------------------------------------- testOptimizePullupWheresW5")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeField("q", new MongoQueryNodeExists),
                new MongoQueryNodeUnion(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeExists),
                    new MongoQueryNodeField("t", new MongoQueryNodeExists)))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists),
                    new MongoQueryNodeField("t", new MongoQueryNodeExists)))
            ))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW6() {
        println("------------------------------------------------- testOptimizePullupWheresW6")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeExists),
                new MongoQueryNodeField("q", new MongoQueryNodeExists),
                new MongoQueryNodeUnion(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeExists),
                    new MongoQueryNodeField("t", new MongoQueryNodeExists)))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeExists))),
                new MongoQueryNodeField("r", new MongoQueryNodeExists),
                new MongoQueryNodeField("s", new MongoQueryNodeExists),
                new MongoQueryNodeField("t", new MongoQueryNodeExists)))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex() {
        println("------------------------------------------------- testOptimizePullupWheresComplex")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeExists),
                        new MongoQueryNodeWhere("W")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("W")))))

        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex2() {
        println("------------------------------------------------- testOptimizePullupWheresComplex2")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeExists),
                    new MongoQueryNodeAnd(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeExists),
                        new MongoQueryNodeWhere("W")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                        new MongoQueryNodeField("a4", new MongoQueryNodeExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                    new MongoQueryNodeField("b1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("b2", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("W")))))

        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex3() {
        println("------------------------------------------------- testOptimizePullupWheresComplex3")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("W1"),
                    new MongoQueryNodeAnd(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeExists),
                        new MongoQueryNodeWhere("W2")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("a3", new MongoQueryNodeExists),
                        new MongoQueryNodeField("a4", new MongoQueryNodeExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                    new MongoQueryNodeField("b1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("b2", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("W2"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeExists),
                    new MongoQueryNodeWhere("W1")))))

        assertEquals(optimizedNode, node.optimize)
    }
}
