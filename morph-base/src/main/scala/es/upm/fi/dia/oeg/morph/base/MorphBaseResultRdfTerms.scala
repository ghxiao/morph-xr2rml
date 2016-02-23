package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.rdf.model.RDFNode

class MorphBaseResultRdfTerms(
        val subjects: List[RDFNode],
        val predicates: List[RDFNode],
        val objects: List[RDFNode],
        val graphs: List[RDFNode]) {
}