package fr.unice.i3s.morph.xr2rml.mongo.query

import org.junit.Assert.assertEquals
import org.junit.Test
import es.upm.fi.dia.oeg.morph.base.GeneralUtility

class MongoQueryNodeOptimizeTest {

    @Test def testOptimizeFlattenAnds() {
        println("------------------------------------------------- testOptimizeFlattenAnds")
        val node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists)))))
        println(node.toString)
        // Raw
        assertEquals(GeneralUtility.cleanString("""$and: [{'p': {$exists: true}}, {$and: [{'q': {$exists: true}}, {'r': {$exists: true}}]}]"""), GeneralUtility.cleanString(node.toString))
        // Optimized
        assertEquals(GeneralUtility.cleanString("""$and: [{'p': {$exists: true}}, {'q': {$exists: true}}, {'r': {$exists: true}}]"""), GeneralUtility.cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizeFlattenOrs() {
        println("------------------------------------------------- testOptimizeFlattenOrs")
        val node: MongoQueryNode =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists)))))
        println(node.toString)
        // Raw
        assertEquals(GeneralUtility.cleanString("""$or: [{'p': {$exists: true}}, {$or: [{'q': {$exists: true}}, {'r': {$exists: true}}]}]"""), GeneralUtility.cleanString(node.toString))
        // Optimized
        assertEquals(GeneralUtility.cleanString("""$or: [{'p': {$exists: true}}, {'q': {$exists: true}}, {'r': {$exists: true}}]"""), GeneralUtility.cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizeFlattenUnions() {
        println("------------------------------------------------- testOptimizeFlattenUnions")
        val node: MongoQueryNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeUnion(
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists)))
        println(node.toString)
        // Raw
        assertEquals(GeneralUtility.cleanString("""UNION: [{'p': {$exists: true}}, {UNION: [{'q': {$exists: true}}, {'r': {$exists: true}}]}]"""), GeneralUtility.cleanString(node.toString))
        // Optimized
        assertEquals(GeneralUtility.cleanString("""UNION: [{'p': {$exists: true}}, {'q': {$exists: true}}, {'r': {$exists: true}}]"""), GeneralUtility.cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizeGroupWheres() {
        println("------------------------------------------------- testOptimizeGroupWheres")
        var node: MongoQueryNode =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeWhere("a == b"),
                new MongoQueryNodeWhere("@.q < @.p")))
        println(node.toString)
        // Raw
        assertEquals(GeneralUtility.cleanString("""$and: [{'p': {$exists: true}}, {$where: 'a == b'}, {$where: '@.q < @.p'}]"""), GeneralUtility.cleanString(node.toString))
        // Optimized
        assertEquals(GeneralUtility.cleanString("""$and: [{'p': {$exists: true}}, {$where: '(a == b) && (@.q < @.p)'}]"""), GeneralUtility.cleanString(node.optimizeQuery.toString))

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeWhere("a == b"),
                new MongoQueryNodeWhere("@.q < @.p")))
        println(node.toString)
        // Raw
        assertEquals(GeneralUtility.cleanString("""$or: [{'p': {$exists: true}}, {$where: 'a == b'}, {$where: '@.q < @.p'}]"""), GeneralUtility.cleanString(node.toString))
        // Optimized
        assertEquals(GeneralUtility.cleanString("""$or: [{'p': {$exists: true}}, {$where: '(a == b) || (@.q < @.p)'}]"""), GeneralUtility.cleanString(node.optimizeQuery.toString))
    }

    @Test def testOptimizePullupWheresW1() {
        println("------------------------------------------------- testOptimizePullupWheresW1")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                new MongoQueryNodeWhere("@.p == 1")))
        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists))),
                new MongoQueryNodeWhere("@.p == 1"))
        assertEquals(optimizedNode, node.optimize)

        node = new MongoQueryNodeOr(List(
            new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
            new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
            new MongoQueryNodeWhere("@.p == 1"),
            new MongoQueryNodeWhere("@.q > 2")))
        optimizedNode = new MongoQueryNodeUnion(
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("q", new MongoQueryNodeCondExists))),
            new MongoQueryNodeWhere("(@.p == 1) || (@.q > 2)"))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW2() {
        println("------------------------------------------------- testOptimizePullupWheresW2")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("@.p == 1")))))

        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("@.p == 1"))))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW4() {
        println("------------------------------------------------- testOptimizePullupWheresW4")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("@.p == 1")))))

        optimizedNode =
            new MongoQueryNodeUnion(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("s", new MongoQueryNodeCondExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("@.p == 1"))))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW5() {
        println("------------------------------------------------- testOptimizePullupWheresW5")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                new MongoQueryNodeUnion(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("t", new MongoQueryNodeCondExists)))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeCondExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("t", new MongoQueryNodeCondExists)))
            ))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresW6() {
        println("------------------------------------------------- testOptimizePullupWheresW6")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("q", new MongoQueryNodeCondExists),
                new MongoQueryNodeUnion(List(
                    new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("s", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("t", new MongoQueryNodeCondExists)))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("p", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("q", new MongoQueryNodeCondExists))),
                new MongoQueryNodeField("r", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("s", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("t", new MongoQueryNodeCondExists)))
        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex() {
        println("------------------------------------------------- testOptimizePullupWheresComplex")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeOr(List(
                new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeCondExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeCondExists),
                        new MongoQueryNodeWhere("W")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeCondExists))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeCondExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeCondExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("W")))))

        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex2() {
        println("------------------------------------------------- testOptimizePullupWheresComplex2")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeCondExists),
                    new MongoQueryNodeAnd(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeCondExists),
                        new MongoQueryNodeWhere("W")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("a4", new MongoQueryNodeCondExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("b1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("b2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("W")))))

        assertEquals(optimizedNode, node.optimize)
    }

    @Test def testOptimizePullupWheresComplex3() {
        println("------------------------------------------------- testOptimizePullupWheresComplex3")

        var node, optimizedNode: MongoQueryNode = null

        node =
            new MongoQueryNodeAnd(List(
                new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                new MongoQueryNodeOr(List(
                    new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a4", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("W1"),
                    new MongoQueryNodeAnd(List(
                        new MongoQueryNodeField("b1", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("b2", new MongoQueryNodeCondExists),
                        new MongoQueryNodeWhere("W2")))))))

        optimizedNode =
            new MongoQueryNodeUnion(List(
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeOr(List(
                        new MongoQueryNodeField("a3", new MongoQueryNodeCondExists),
                        new MongoQueryNodeField("a4", new MongoQueryNodeCondExists))))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("b1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("b2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("W2"))),
                new MongoQueryNodeAnd(List(
                    new MongoQueryNodeField("a1", new MongoQueryNodeCondExists),
                    new MongoQueryNodeField("a2", new MongoQueryNodeCondExists),
                    new MongoQueryNodeWhere("W1")))))

        assertEquals(optimizedNode, node.optimize)
    }
}
