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

abstract class MorphBaseQueryTranslator extends IQueryTranslator {

    val logger = Logger.getLogger(this.getClass());

    /** Map of nodes of a SPARQL query and the candidate triples maps for each node **/
    var mapInferredTMs: Map[Node, Set[R2RMLTriplesMap]] = Map.empty;

    Optimize.setFactory(new MorphQueryRewritterFactory());

    /**
     * High level method to start the translation process.
     * The result is a UnionOfGenericQueries, i.e. a set of concrete queries
     * whereof results must be UNIONed.<br>
     * Since SQL supports UNION with no restriction, in the case of a RDB the result consists
     * of exactly one query.<br>
     * Conversely, MongoDB does support UNIONs (by means of the $or operator), but only if there
     * is no $where as members of the $or. Therefore it is not always possible to create a since
     * MongoDB query that is equivalent to the SPARQL query. In this case, several concrete queries
     * are returned, and the xR2RML processor shall compute the union itself.
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