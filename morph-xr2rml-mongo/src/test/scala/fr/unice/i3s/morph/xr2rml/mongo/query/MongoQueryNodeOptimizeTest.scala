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
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeExists("r")))))
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
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeExists("r")))))
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
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeUnion(
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeExists("r")))
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
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeWhere("a == b"),
                new MongoQueryNodeWhere("@.q < @.p")))
        println(node.toString)
        // Raw
        assertEquals(cleanString("""$and: [{'p': {$exists: true}}, {$where: 'a == b'}, {$where: '@.q < @.p'}]"""), cleanString(node.toString))
        // Optimized
        assertEquals(cleanString("""$and: [{'p': {$exists: true}}, {$where: '(a == b) && (@.q < @.p)'}]"""), cleanString(node.optimizeQuery.toString))

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeExists("p"),
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
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeExists("q"),
                new MongoQueryNodeWhere("@.p == 1")))
        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"))),
                new MongoQueryNodeWhere("@.p == 1"))
        assertEquals(optimizedNode, node.optimize)

        node = new MongoQueryNodeOr(List(
            new MongoQueryNodeExists("p"),
            new MongoQueryNodeExists("q"),
            new MongoQueryNodeWhere("@.p == 1"),
            new MongoQueryNodeWhere("@.q > 2")))
        optimizedNode = new MongoQueryNodeUnion(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeExists("q"))),
            new MongoQueryNodeWhere("(@.p == 1) || (@.q > 2)"))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW2() {
        println("------------------------------------------------- testOptimizePullupWheresW2")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeExists("q"),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("r"),
                    new MongoQueryNodeExists("s"),
                    new MongoQueryNodeWhere("@.p == 1")))))

        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("r"),
                    new MongoQueryNodeExists("s"),
                    new MongoQueryNodeWhere("@.p == 1"))))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW4() {
        println("------------------------------------------------- testOptimizePullupWheresW4")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeExists("q"),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("r"),
                    new MongoQueryNodeExists("s"),
                    new MongoQueryNodeWhere("@.p == 1")))))

        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeExists("r"),
                        new MongoQueryNodeExists("s"))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeWhere("@.p == 1"))))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW5() {
        println("------------------------------------------------- testOptimizePullupWheresW5")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeExists("q"),
                new MongoQueryNodeUnion(List(
                    new MongoQueryNodeExists("r"),
                    new MongoQueryNodeExists("s"),
                    new MongoQueryNodeExists("t")))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeExists("r"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeExists("s"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"),
                    new MongoQueryNodeExists("t")))
            ))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW6() {
        println("------------------------------------------------- testOptimizePullupWheresW6")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeExists("p"),
                new MongoQueryNodeExists("q"),
                new MongoQueryNodeUnion(List(
                    new MongoQueryNodeExists("r"),
                    new MongoQueryNodeExists("s"),
                    new MongoQueryNodeExists("t")))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("p"),
                    new MongoQueryNodeExists("q"))),
                new MongoQueryNodeExists("r"),
                new MongoQueryNodeExists("s"),
                new MongoQueryNodeExists("t")))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex() {
        println("------------------------------------------------- testOptimizePullupWheresComplex")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeExists("a1"),
                new MongoQueryNodeExists("a2"),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a3"),
                    new MongoQueryNodeExists("a4"),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeExists("b1"),
                        new MongoQueryNodeExists("b2"),
                        new MongoQueryNodeWhere("W")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("a1"),
                    new MongoQueryNodeExists("a2"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a3"),
                    new MongoQueryNodeExists("a4"),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeExists("b1"),
                        new MongoQueryNodeExists("b2"))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a3"),
                    new MongoQueryNodeExists("a4"),
                    new MongoQueryNodeWhere("W")))))

        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex2() {
        println("------------------------------------------------- testOptimizePullupWheresComplex2")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeExists("a1"),
                new MongoQueryNodeExists("a2"),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("a3"),
                    new MongoQueryNodeExists("a4"),
                    new MongoQueryNodeAnd(List(
                        new MongoQueryNodeExists("b1"),
                        new MongoQueryNodeExists("b2"),
                        new MongoQueryNodeWhere("W")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a1"),
                    new MongoQueryNodeExists("a2"),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeExists("a3"),
                        new MongoQueryNodeExists("a4"))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a1"),
                    new MongoQueryNodeExists("a2"),
                    new MongoQueryNodeExists("b1"),
                    new MongoQueryNodeExists("b2"),
                    new MongoQueryNodeWhere("W")))))

        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex3() {
        println("------------------------------------------------- testOptimizePullupWheresComplex3")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeExists("a1"),
                new MongoQueryNodeExists("a2"),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeExists("a3"),
                    new MongoQueryNodeExists("a4"),
                    new MongoQueryNodeWhere("W1"),
                    new MongoQueryNodeAnd(List(
                        new MongoQueryNodeExists("b1"),
                        new MongoQueryNodeExists("b2"),
                        new MongoQueryNodeWhere("W2")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a1"),
                    new MongoQueryNodeExists("a2"),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeExists("a3"),
                        new MongoQueryNodeExists("a4"))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a1"),
                    new MongoQueryNodeExists("a2"),
                    new MongoQueryNodeExists("b1"),
                    new MongoQueryNodeExists("b2"),
                    new MongoQueryNodeWhere("W2"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeExists("a1"),
                    new MongoQueryNodeExists("a2"),
                    new MongoQueryNodeWhere("W1")))))

        assertEquals(optimizedNode, node.optimize)
    }
}
