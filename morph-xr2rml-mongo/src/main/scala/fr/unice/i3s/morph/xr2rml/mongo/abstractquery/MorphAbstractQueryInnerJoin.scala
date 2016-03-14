package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.MorphMongoQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Representation of the INNER JOIN abstract query generated from two basic graph patterns.
 *
 * @param left the query representing the left basic graph pattern of the join
 * @param right the query representing the right basic graph pattern of the join
 */
class MorphAbstractQueryInnerJoin(

    val left: MorphAbstractQuery,
    val right: MorphAbstractQuery)

        extends MorphAbstractQuery(Set.empty) {

    val logger = Logger.getLogger(this.getClass().getName());

    override def toString = {
        "[" + left.toString + "\n" +
            "] INNER JOIN [\n" +
            right.toString + "\n" +
            "] ON " + getSharedVariables
    }

    override def toStringConcrete: String = {
        "[" + left.toStringConcrete + "\n" +
            "] INNER JOIN [\n" +
            right.toStringConcrete + "\n" +
            "] ON " + getSharedVariables
    }

    /**
     * Translate all atomic abstract queries of this abstract query into concrete queries.
     * @param translator the query translator
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        left.translateAtomicAbstactQueriesToConcrete(translator)
        right.translateAtomicAbstactQueriesToConcrete(translator)
    }

    /**
     * Check if atomic abstract queries within this query have a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = {
        left.isTargetQuerySet && right.isTargetQuerySet
    }

    /**
     * Return the list of SPARQL variables projected in this abstract query
     */
    override def getVariables: Set[String] = {
        (left.getVariables ++ right.getVariables)
    }

    /**
     * Execute the left and right queries, generate the RDF terms for each of the result documents,
     * then make a JOIN of all the results
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     * @throws MorphException if the triples map bound to the query has no referencing object map
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        logger.info("Generating RDF triples from the inner join query:\n" + this.toStringConcrete)
        val joinResult: scala.collection.mutable.Map[String, MorphBaseResultRdfTerms] = new scala.collection.mutable.HashMap

        // First, generate the triples for both left and right graph patterns of the join
        val leftTriples = left.generateRdfTerms(dataSourceReader, dataTranslator)
        val rightTriples = right.generateRdfTerms(dataSourceReader, dataTranslator)
        var nonJoinedLeft = leftTriples
        var nonJoinedRight = rightTriples

        if (logger.isDebugEnabled)
            logger.debug("Inner joining " + leftTriples.size + " left triples with " + rightTriples.size + " right triples.")
        if (logger.isTraceEnabled) {
            logger.trace("Left triples:\n" + leftTriples.mkString("\n"))
            logger.trace("Right triples:\n" + rightTriples.mkString("\n"))
        }

        val sharedVars = this.getSharedVariables

        if (sharedVars.isEmpty) {
            val res = leftTriples ++ rightTriples // no filtering if no common variable
            logger.info("Inner join on empty set of variables computed " + res.size + " triples.")
            res
        } else {
            // For each variable x shared by both graph patterns, select the left and right triples
            // in which at least one term is bound to x, then join the documents on these terms.
            for (x <- sharedVars) {

                val leftTripleX = leftTriples.filter(_.hasVariable(x))
                val rightTripleX = rightTriples.filter(_.hasVariable(x))

                for (leftTriple <- leftTripleX) {
                    val leftTerm = leftTriple.getTermsForVariable(x)
                    for (rightTriple <- rightTripleX) {
                        val rightTerm = rightTriple.getTermsForVariable(x)
                        if (!leftTerm.intersect(rightTerm).isEmpty) {
                            // If there is a match, keep the two MorphBaseResultRdfTerms instances 
                            if (!joinResult.contains(leftTriple.getId))
                                joinResult += (leftTriple.getId -> leftTriple)
                            if (!joinResult.contains(rightTriple.getId))
                                joinResult += (rightTriple.getId -> rightTriple)
                        }
                    }
                }

                // All left and right triples that do not contain any of the shared variables are kept
                nonJoinedLeft = nonJoinedLeft.filter(!_.hasVariable(x))
                nonJoinedRight = nonJoinedRight.filter(!_.hasVariable(x))
            }

            val res = joinResult.values.toList
            val resNonJoined = nonJoinedLeft ++ nonJoinedLeft
            logger.info("Inner join computed " + res.size + " triples + " + resNonJoined.size + " triples with no shared variable.")
            res ++ resNonJoined
        }
    }

    private def getSharedVariables = {
        left.getVariables.intersect(right.getVariables)
    }

    /**
     * Optimize left and right members and try to merge them if they are atomic queries
     */
    override def optimizeQuery: MorphAbstractQuery = {

        val leftOpt = left.optimizeQuery
        val rightOpt = right.optimizeQuery

        // Inner join of 2 atomic queries
        if (leftOpt.isInstanceOf[MorphAbstractAtomicQuery] && rightOpt.isInstanceOf[MorphAbstractAtomicQuery]) {
            val opt = leftOpt.asInstanceOf[MorphAbstractAtomicQuery].mergeForInnerJoin(rightOpt.asInstanceOf[MorphAbstractAtomicQuery])
            if (opt.isDefined) {
                if (logger.isDebugEnabled())
                    logger.debug("\n------------------ Optimized ------------------\n" + this.toString + "\n------------------ into ------------------ \n" + opt.get.toString)
                return opt.get
            }
        }

        // Inner join of one atomic query (left) and an inner join (right)
        if (leftOpt.isInstanceOf[MorphAbstractAtomicQuery] && rightOpt.isInstanceOf[MorphAbstractQueryInnerJoin]) {
            val leftAtom = leftOpt.asInstanceOf[MorphAbstractAtomicQuery]
            val rightInnerJ = rightOpt.asInstanceOf[MorphAbstractQueryInnerJoin]

            if (rightInnerJ.left.isInstanceOf[MorphAbstractAtomicQuery]) {
                // Merge left and right.left
                val opt = leftAtom.mergeForInnerJoin(rightInnerJ.left.asInstanceOf[MorphAbstractAtomicQuery])
                if (opt.isDefined) {
                    val result = new MorphAbstractQueryInnerJoin(opt.get, rightInnerJ.right)
                    if (logger.isDebugEnabled())
                        logger.debug("\n------------------ Optimized ------------------\n" + this.toString + "\n------------------ into ------------------ \n" + result.toString)
                    return result
                }
            }
            if (rightInnerJ.right.isInstanceOf[MorphAbstractAtomicQuery]) {
                // Merge left and right.right
                val opt = leftAtom.mergeForInnerJoin(rightInnerJ.right.asInstanceOf[MorphAbstractAtomicQuery])
                if (opt.isDefined) {
                    val result = new MorphAbstractQueryInnerJoin(opt.get, rightInnerJ.left)
                    if (logger.isDebugEnabled())
                        logger.debug("\n------------------ Optimized ------------------\n" + this.toString + "\n------------------ into ------------------ \n" + result.toString)
                    return result
                }
            }
        }

        // Inner join of one inner (left) and one atomic query (right)
        if (leftOpt.isInstanceOf[MorphAbstractQueryInnerJoin] && rightOpt.isInstanceOf[MorphAbstractAtomicQuery]) {
            val leftInnerJ = leftOpt.asInstanceOf[MorphAbstractQueryInnerJoin]
            val rightAtom = rightOpt.asInstanceOf[MorphAbstractAtomicQuery]

            if (leftInnerJ.left.isInstanceOf[MorphAbstractAtomicQuery]) {
                // Merge left.left and right
                val opt = leftInnerJ.left.asInstanceOf[MorphAbstractAtomicQuery].mergeForInnerJoin(rightAtom)
                if (opt.isDefined) {
                    val result = new MorphAbstractQueryInnerJoin(opt.get, leftInnerJ.right).optimizeQuery
                    if (logger.isDebugEnabled())
                        logger.debug("\n------------------ Optimized ------------------\n" + this.toString + "\n------------------ into ------------------ \n" + result.toString)
                    return result
                }
            }
            if (leftInnerJ.right.isInstanceOf[MorphAbstractAtomicQuery]) {
                // Merge left.right and right
                val opt = leftInnerJ.right.asInstanceOf[MorphAbstractAtomicQuery].mergeForInnerJoin(rightAtom)
                if (opt.isDefined) {
                    val result = new MorphAbstractQueryInnerJoin(opt.get, leftInnerJ.left).optimizeQuery
                    if (logger.isDebugEnabled())
                        logger.debug("\n------------------ Optimized ------------------\n" + this.toString + "\n------------------ into ------------------ \n" + result.toString)
                    return result
                }
            }
        }

        val res = new MorphAbstractQueryInnerJoin(leftOpt, rightOpt)
        if (logger.isDebugEnabled())
            logger.debug("\n------------------ Optimized ------------------\n" + this.toString + "\n------------------ into ------------------ \n" + res.toString)
        res
    }
}