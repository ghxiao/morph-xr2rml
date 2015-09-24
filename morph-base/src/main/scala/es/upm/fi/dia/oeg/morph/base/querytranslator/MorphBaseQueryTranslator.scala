package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize

import es.upm.fi.dia.oeg.morph.base.UnionOfGenericQueries
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryRewritterFactory
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Abstract class for the engine that shall translate a SPARQL query into a concrete database query
 */
abstract class MorphBaseQueryTranslator extends IQueryTranslator {

    val logger = Logger.getLogger(this.getClass());

    /** Map of nodes of a SPARQL query and the candidate triples maps for each node **/
    var mapInferredTMs: Map[Node, Set[R2RMLTriplesMap]] = Map.empty;

    Optimize.setFactory(new MorphQueryRewritterFactory());

    /**
     * <p>High level method to start the translation process.
     * The result is a UnionOfGenericQueries, i.e. two sets of concrete queries: a child and optionally a parent set. 
     * Within each set, results of all concrete queries must be UNIONed.</p>
     * 
     * <p>In the RDB case, the UnionOfGenericQueries is a bit exaggerated ;-): it should contain only
     * a child query (there is no need to split child and parent queries since SQL supports joins),
     * and exactly one element in the child query (since SQL supports the UNION).</p>
     *
     * <p>Conversely, MongoDB does not support the JOIN, therefore there may be a child <em>and</em> a parent query.
     * MongoDB does support UNIONs (by means of the $or operator), but only if there is no $where as members of the $or.
     * Therefore it is not always possible to create a single MongoDB query that is equivalent to the SPARQL query. 
     * In this case, several concrete queries are returned, and the xR2RML processor shall compute the union itself.</p>
     *
     * @param sparqlQuery the SPARQL query to translate
     * @return set of concrete database queries of which results of be "UNIONed". 
     * The result may be empty but not null. 
     */
    def translate(sparqlQuery: Query): UnionOfGenericQueries = {

        val start = System.currentTimeMillis()
        val result = this.translate(Algebra.compile(sparqlQuery));
        logger.info("Query translation time = " + (System.currentTimeMillis() - start) + "ms.");
        result
    }

    protected def translate(op: Op): UnionOfGenericQueries
}