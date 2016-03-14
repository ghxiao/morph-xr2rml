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
 * Representation of the LEFT JOIN abstract query generated from two basic graph patterns.
 *
 * @param left the query representing the left basic graph pattern of the join
 * @param right the query representing the right basic graph pattern of the join
 */
class MorphAbstractQueryLeftJoin(

    val left: MorphAbstractQuery,
    val right: MorphAbstractQuery)

        extends MorphAbstractQuery(Set.empty) {

    val logger = Logger.getLogger(this.getClass().getName());

    override def toString = {
        "[" + left.toString + "\n" +
            "] LEFt JOIN [\n" +
            right.toString + "\n" +
            "] ON " + getSharedVariables
    }

    override def toStringConcrete: String = {
        "[" + left.toStringConcrete + "\n" +
            "] LEFT JOIN [\n" +
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
     * then make a LEFT JOIN of all the results
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

        logger.info("Generating RDF terms from the left join query:\n" + this.toStringConcrete)
        val joinResult: scala.collection.mutable.Map[String, MorphBaseResultRdfTerms] = new scala.collection.mutable.HashMap

        // First, generate the triples for both left and right graph patterns of the join
        val leftTriples = left.generateRdfTerms(dataSourceReader, dataTranslator)
        val rightTriples = right.generateRdfTerms(dataSourceReader, dataTranslator)
        var nonJoinedLeft = leftTriples
        var nonJoinedRight = rightTriples

        if (logger.isDebugEnabled)
            logger.debug("Left joining " + leftTriples.size + " left triple(s) with " + rightTriples.size + " right triple(s).")
        if (logger.isTraceEnabled) {
            logger.trace("Left triples:\n" + leftTriples.mkString("\n"))
            logger.trace("Right triples:\n" + rightTriples.mkString("\n"))
        }

        val sharedVars = this.getSharedVariables
        
        if (sharedVars.isEmpty) {
            val res = leftTriples ++ rightTriples // no filtering if no common variable
            logger.info("Left join on empty set of variables computed " + res.size + " triples.")
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
                        if (leftTerm.intersect(rightTerm).isEmpty) {
                            // If there is no match, keep only the left MorphBaseResultRdfTerms instances 
                            if (!joinResult.contains(leftTriple.getId))
                                joinResult += (leftTriple.getId -> leftTriple)
                        } else {
                            // If there is a match, keep the left and right MorphBaseResultRdfTerms instances 
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
            logger.info("Left join computed " + res.size + " triples + " + resNonJoined.size + " triples with no shared variable.")
            res ++ resNonJoined
        }
    }

    private def getSharedVariables: Set[String] = {
        left.getVariables.intersect(right.getVariables)
    }

    /**
     * Optimize left and right members and try to merge them if they are atomic queries
     */
    override def optimizeQuery: MorphAbstractQuery = {

        val leftOpt = left.optimizeQuery
        val rightOpt = right.optimizeQuery
        if (leftOpt.isInstanceOf[MorphAbstractAtomicQuery] && rightOpt.isInstanceOf[MorphAbstractAtomicQuery]) {
            val opt = leftOpt.asInstanceOf[MorphAbstractAtomicQuery].mergeForLeftJoin(rightOpt.asInstanceOf[MorphAbstractAtomicQuery])
            if (opt.isDefined) return opt.get
        }

        new MorphAbstractQueryLeftJoin(leftOpt, rightOpt)
    }
}