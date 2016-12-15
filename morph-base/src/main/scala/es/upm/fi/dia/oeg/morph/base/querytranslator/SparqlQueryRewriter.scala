package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.ARQ
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpNull
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpTable
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import com.hp.hpl.jena.sparql.util.Context

/**
 * Apply various rewriting and optimization on the SPARQL query, either Jena std optimizations
 * or specific optimizations.
 */
object SparqlQueryRewriter {

    def logger = Logger.getLogger(this.getClass.getName)

    val context = new Context()
    context.put(ARQ.optPathFlatten, true)
    context.put(ARQ.optFilterPlacement, true)
    context.put(ARQ.optFilterPlacementBGP, false)
    context.put(ARQ.optFilterPlacementConservative, true)
    context.put(ARQ.optTopNSorting, true)
    context.put(ARQ.optDistinctToReduced, true) // for constant-projection optimization to work properly
    context.put(ARQ.optOrderByDistinctApplication, true)
    context.put(ARQ.optFilterEquality, true)
    context.put(ARQ.optFilterInequality, true)
    context.put(ARQ.optFilterImplicitJoin, true)
    context.put(ARQ.optImplicitLeftJoin, true)
    context.put(ARQ.optTermStrings, false)
    context.put(ARQ.optExprConstantFolding, true)
    context.put(ARQ.optFilterConjunction, true)
    context.put(ARQ.optFilterExpandOneOf, true)
    context.put(ARQ.optFilterDisjunction, true)
    context.put(ARQ.optPromoteTableEmpty, false)
    context.put(ARQ.optIndexJoinStrategy, false)
    context.put(ARQ.optMergeBGPs, true)
    context.put(ARQ.optMergeExtends, true)

    val rewriter = Optimize.getFactory().create(context)

    /**
     * Apply various rewriting and optimization on the SPARQL query,
     * either Jena std optimizations or specific optimizations
     */
    def rewriteOptimize(sparqlQuery: Query, sparqlOptimization: Boolean): Op = {

        val op = Algebra.compile(sparqlQuery)

        val result =
            if (sparqlOptimization) {
                // Perform Jena optimizations
                val op2 = rewriter.rewrite(op)
                if (logger.isDebugEnabled()) {
                    logger.debug("Compiled SPARQL query:\n" + op)
                    if (op != op2)
                        logger.debug("Jena optimizer rewrote query to: \n" + op2)
                }

                // Perform other optimizations
                val op3 = this.rewrite(op2)
                if (logger.isDebugEnabled() && (op3 != op2))
                    logger.debug("Local optimizer rewrote query to: \n" + op3)
                op3
            } else op

        if (logger.isInfoEnabled())
            if (result != op)
                logger.info("Compiled SPARQL graph pattern:\n" + op + "-- rewritten/optimized to --\n" + result)
            else
                logger.info("SPARQL graph pattern:\n" + result)
        result
    }

    /**
     * Expand query<br>
     *   DESCRIBE <uri><br>
     * to<br>
     *   DESCRIBE <uri> WHERE { { <uri> ?p ?x. } UNION { ?y ?q <uri> . } }<br>
     *
     * @note
     * sparqlQuery.getResultURIs: static URIs in the SELECT/DESCRIBE clause<br>
     * sparqlQuery.getResultVars: variables in the SELECT/DESCRIBE clause (as strings e.g. "x")<br>
     * sparqlQuery.getProjectVars: variables in the SELECT/DESCRIBE clause (as instances of Var)<br>
     * sparqlQuery.isQueryResultStar: true if the '*' is in the SELECT/DESCRIBE clause<br>
     *
     * @param sparqlQuery
     * @return the same query if no change or a query with update graph pattern
     */
    def expandDescribe(sparqlQuery: Query): Query = {

        if (sparqlQuery.isDescribeType) {
            if (sparqlQuery.getProjectVars.isEmpty && !sparqlQuery.getResultURIs.isEmpty) {
                var op = Algebra.compile(sparqlQuery)
                if (op.isInstanceOf[OpTable] || op.isInstanceOf[OpNull]) {

                    val listUris = sparqlQuery.getResultURIs().toList
                    var idx = 1
                    val union = new ElementUnion

                    listUris.foreach(uri => {
                        val bgp1 = new ElementTriplesBlock()
                        // <uri> ?p ?x
                        bgp1.addTriple(Triple.create(uri, Var.alloc("p" + idx), Var.alloc("x" + idx)))
                        val bgp2 = new ElementTriplesBlock()
                        // ?y ?q <uri>
                        bgp2.addTriple(Triple.create(Var.alloc("y" + idx), Var.alloc("q" + idx), uri))

                        union.addElement(bgp1)
                        union.addElement(bgp2)
                        idx += 1
                    })

                    val body = new ElementGroup()
                    body.addElement(union);
                    sparqlQuery.setQueryPattern(body)
                    if (logger.isDebugEnabled()) logger.debug("Expanded query to: \n" + sparqlQuery)
                }
            }
        }
        sparqlQuery
    }

    def rewrite(op: Op): Op = {
        op match {
            case bgp: OpBGP => {
                this.rewriteBGP(bgp)
            }
            case opJoin: OpJoin => { // AND pattern
                this.rewriteJoin(opJoin)
            }
            case opLeftJoin: OpLeftJoin => { // OPT pattern
                this.rewriteLeftJoin(opLeftJoin)
            }
            case opUnion: OpUnion => { // UNION pattern
                this.rewriteUnion(opUnion)
            }
            case opFilter: OpFilter => { // FILTER pattern
                val exprs = opFilter.getExprs
                val subOp = opFilter.getSubOp
                OpFilter.filter(exprs, this.rewrite(subOp))
            }
            case opProject: OpProject => {
                val subOp = opProject.getSubOp
                new OpProject(this.rewrite(subOp), opProject.getVars)
            }
            case opSlice: OpSlice => {
                val subOp = opSlice.getSubOp
                new OpSlice(this.rewrite(subOp), opSlice.getStart, opSlice.getLength)
            }
            case opDistinct: OpDistinct => {
                val subOp = opDistinct.getSubOp
                new OpDistinct(this.rewrite(subOp))
            }
            case opOrder: OpOrder => {
                val subOp = opOrder.getSubOp
                new OpOrder(this.rewrite(subOp), opOrder.getConditions)
            }
            case _ => {
                op
            }
        }
    }

    /**
     * Reorder triples of a BGP by subject
     */
    private def rewriteBGP(bgp: OpBGP): Op = {
        SparqlUtility.groupBGPBySubject(bgp)
    }

    private def rewriteUnion(opUnion: OpUnion): Op = {
        val left = opUnion.getLeft
        val right = opUnion.getRight
        OpUnion.create(this.rewrite(left), this.rewrite(right))
    }

    /**
     * Rewrite the two members of a JOIN.
     * In case both are BGPs, merge them into a single one (should be already performed by Jena optimization)
     */
    private def rewriteJoin(opJoin: OpJoin): Op = {

        val leftRewritten = this.rewrite(opJoin.getLeft)
        val rightRewritten = this.rewrite(opJoin.getRight)

        if (leftRewritten.isInstanceOf[OpBGP] && rightRewritten.isInstanceOf[OpBGP]) {
            // Merge two joined BGPs into a BGP and reorder triples
            val leftRewrittenTriples = leftRewritten.asInstanceOf[OpBGP].getPattern().getList().toList
            val rightRewrittenTriples = rightRewritten.asInstanceOf[OpBGP].getPattern().getList().toList

            val reordTpList = SparqlUtility.groupTriplesBySubject(leftRewrittenTriples ::: rightRewrittenTriples)
            new OpBGP(BasicPattern.wrap(reordTpList))
        } else
            OpJoin.create(leftRewritten, rightRewritten)
    }

    private def rewriteLeftJoin(opLeftJoin: OpLeftJoin): Op = {
        val left = opLeftJoin.getLeft
        val right = opLeftJoin.getRight
        val exprList = opLeftJoin.getExprs
        OpLeftJoin.create(this.rewrite(left), this.rewrite(right), exprList)
    }
}
