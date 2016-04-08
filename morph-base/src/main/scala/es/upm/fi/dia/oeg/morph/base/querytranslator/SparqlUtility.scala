package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.core.BasicPattern

import es.upm.fi.dia.oeg.morph.base.Constants

object SparqlUtility {
    val logger = Logger.getLogger(this.getClass().getName());

    def groupTriplesBySubject(triples: java.util.List[Triple]): java.util.List[Triple] = {
        var result: List[Triple] = Nil;
        if (triples == null || triples.size() == 0) {
            result = Nil;
        } else {
            if (triples.size() == 1) {
                result = triples.toList;
            } else {
                var resultAux: Map[Node, List[Triple]] = Map.empty;

                var subjectSet: Set[Node] = Set.empty;

                for (tp <- triples) {
                    val tpSubject = tp.getSubject();

                    if (resultAux.contains(tpSubject)) {
                        var triplesBySubject = resultAux(tpSubject);
                        triplesBySubject = triplesBySubject ::: List(tp)
                        resultAux += (tpSubject -> triplesBySubject);
                    } else {
                        val triplesBySubject: List[Triple] = List(tp);
                        resultAux += (tpSubject -> triplesBySubject);
                    }
                }

                for (triplesBySubject <- resultAux.values()) {
                    result = result ::: triplesBySubject;
                }
            }
        }
        result;
    }

    def groupBGPBySubject(bgp: OpBGP): OpBGP = {
        try {
            val basicPattern = bgp.getPattern();
            var mapTripleHashCode: Map[Integer, List[Triple]] = Map.empty

            for (tp <- basicPattern) {
                val tpSubject = tp.getSubject();

                val tripleSubjectHashCode = new Integer(tpSubject.hashCode());

                if (mapTripleHashCode.containsKey(tripleSubjectHashCode)) {
                    var triplesByHashCode = mapTripleHashCode(tripleSubjectHashCode);
                    triplesByHashCode = triplesByHashCode ::: List(tp);
                    mapTripleHashCode += (tripleSubjectHashCode -> triplesByHashCode);
                } else {
                    val triplesByHashCode: List[Triple] = List(tp);
                    mapTripleHashCode += (tripleSubjectHashCode -> triplesByHashCode);
                }

            }
            var triplesReordered: List[Triple] = Nil;
            for (key <- mapTripleHashCode.keySet) {
                val triplesByHashCode = mapTripleHashCode(key);
                triplesReordered = triplesReordered ::: triplesByHashCode;
            }

            val basicPattern2 = BasicPattern.wrap(triplesReordered);
            val bgp2 = new OpBGP(basicPattern2);
            return bgp2;
        } catch {
            case e: Exception => {
                val errorMessage = "Error while grouping triples, original triples will be returned.";
                logger.warn(errorMessage);
                return bgp;
            }
        }
    }

    def getSubjects(triples: List[Triple]): List[Node] = {
        var result: List[Node] = Nil;

        if (triples != null) {
            for (triple <- triples) {
                result = result ::: List(triple.getSubject());
            }
        }

        result;
    }

    def getObjects(triples: List[Triple]): List[Node] = {
        var result: List[Node] = Nil;

        if (triples != null) {
            for (triple <- triples) {
                result = result ::: List(triple.getObject());
            }
        }

        result;
    }

    def isBlankNode(node: Node) = {
        val result = {
            if (node.isBlank()) {
                true
            } else {
                if (node.isVariable()) {
                    val varName = node.getName();
                    if (varName.startsWith("?")) {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }

        }
        result;
    }

    def isNodeInSubjectTriple(node: Node, tp: Triple): Boolean = {
        tp.getSubject() == node
    }

    def isNodeInSubjectGraph(node: Node, op: Op): Boolean = {
        val found = op match {
            case tp: Triple => {
                this.isNodeInSubjectTriple(node, tp)
            }
            case bgp: OpBGP => {
                this.isNodeInSubjectBGP(node, bgp.getPattern().toList);
            }
            case join: OpJoin => {
                this.isNodeInSubjectGraphs(node, join.getLeft(), join.getRight());
            }
            case leftJoin: OpLeftJoin => {
                this.isNodeInSubjectGraphs(node, leftJoin.getLeft(), leftJoin.getRight());
            }
            case union: OpUnion => {
                this.isNodeInSubjectGraphs(node, union.getLeft(), union.getRight());
            }
            case filter: OpFilter => {
                this.isNodeInSubjectGraph(node, filter.getSubOp());
            }
            case project: OpProject => {
                this.isNodeInSubjectGraph(node, project.getSubOp());
            }
            case slice: OpSlice => {
                this.isNodeInSubjectGraph(node, slice.getSubOp());
            }
            case distinct: OpDistinct => {
                this.isNodeInSubjectGraph(node, distinct.getSubOp());
            }
            case order: OpOrder => {
                this.isNodeInSubjectGraph(node, order.getSubOp());
            }
            case _ => false
        }

        found;
    }

    def isNodeInSubjectBGP(node: Node, bgpList: List[Triple]): Boolean = {
        val isInHead = isNodeInSubjectTriple(node, bgpList.head);
        var found = isInHead;
        if (!found && !bgpList.tail.isEmpty) {
            found = isNodeInSubjectBGP(node, bgpList.tail);
        }
        found;
    }

    def isNodeInSubjectGraphs(node: Node, opLeft: Op, opRight: Op): Boolean = {
        val isInLeft = isNodeInSubjectGraph(node, opLeft);
        var found = isInLeft;
        if (!found) {
            found = isNodeInSubjectGraph(node, opRight);
        }
        found;
    }

    /**
     * Decide the output format and content type of the SPARQL results, based on the SPARQL query
     * and the value of the content-type HTTP header.
     *
     * If the content-type is null or empty, the default RDF syntax or result format is returned depending on the query type.
     *
     * Note: the content-type header may contain several values.
     *
     * @param contentType value of the content-type HTTP header. Can be null.
     * @param query the SPARQL query. Cannot be null.
     * @param defaultRdfSyntax default RDF syntax, should be read from the configuration file. Cannot be null.
     * @param defaultResultFrmt default format for SPARQL result sets, should be read from the configuration file. Cannot be null.
     *
     * @return a couple (negotiated content-type, corresponding format among Constants.OUTPUT_FORMAT*),
     * or None if the requested format cannot be satisfied.<br>
     * Example:
     *   <code>("application/sparql-results+xml", Constants.OUTPUT_FORMAT_RESULT_XML)</code>
     */
    def negotiateContentType(
        contentType: String,
        query: Query,
        defaultRdfSyntax: String,
        defaultResultFrmt: String): Option[(String, String)] = {

        val result = if (query.isAskType || query.isSelectType) {

            // --- Formats for a SPARQL result set
            if (contentType == null || contentType == "") {
                Some((outputFormatToContentType(defaultResultFrmt), defaultResultFrmt))
            } else {
                val ctl = contentType.toLowerCase
                if (ctl contains "application/sparql-results+xml")
                    Some(("application/sparql-results+xml", Constants.OUTPUT_FORMAT_RESULT_XML))
                else if (ctl contains "application/sparql-results+json")
                    Some(("application/sparql-results+json", Constants.OUTPUT_FORMAT_RESULT_JSON))
                else if (ctl contains "application/sparql-results+csv")
                    None
                else if (ctl contains "application/sparql-results+vsv")
                    None
                else
                    None
            }

        } else if (query.isConstructType || query.isDescribeType) {

            // --- RDF formats
            if (contentType == null || contentType == "")
                Some((outputFormatToContentType(defaultRdfSyntax), defaultRdfSyntax))
            else {
                val ctl = contentType.toLowerCase
                if (ctl contains "text/turtle")
                    Some(("text/turtle", Constants.OUTPUT_FORMAT_TURTLE))
                else if (ctl contains "application/rdf+xml")
                    Some(("application/rdf+xml", Constants.OUTPUT_FORMAT_RDFXML))
                else if (ctl contains "text/nt")
                    Some(("text/nt", Constants.OUTPUT_FORMAT_NTRIPLE))
                else if (ctl contains "text/n3")
                    Some(("text/n3", Constants.OUTPUT_FORMAT_N3))
                else
                    None
            }
        } else None

        if (logger.isInfoEnabled)
            logger.info("Request Content-Type: " + contentType + ". Negotiated: " + result.getOrElse("None"))
        result
    }

    private def outputFormatToContentType(outputFormat: String): String = {
        outputFormat match {
            // --- Formats for a SPARQL result set
            case Constants.OUTPUT_FORMAT_RESULT_XML => "application/sparql-results+xml"
            case Constants.OUTPUT_FORMAT_RESULT_JSON => "application/sparql-results+json"

            // --- RDF formats
            case Constants.OUTPUT_FORMAT_TURTLE => "text/turtle"
            case Constants.OUTPUT_FORMAT_RDFXML => "application/rdf+xml"
            case Constants.OUTPUT_FORMAT_RDFXML_ABBREV => "application/rdf+xml"
            case Constants.OUTPUT_FORMAT_NTRIPLE => "text/nt"
            case Constants.OUTPUT_FORMAT_N3 => "text/n3"
            case Constants.OUTPUT_FORMAT_NQUAD => "application/n-quads"

            case _ => ""
        }
    }
}
