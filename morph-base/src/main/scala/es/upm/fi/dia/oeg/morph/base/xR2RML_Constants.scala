package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.rdf.model.ResourceFactory

class xR2RML_Constants {

}

/** xR2RML vocabulary definitions */
object xR2RML_Constants {

    val utilFilesavegraph = "savegraph.xrr"
    val utilFilesave = "save.xrr"
    val utilFilesaveTrash = "savegraphTrash.xrr"

    // Name spaces
    val xR2RML_NS = "http://i3s.unice.fr/xr2rml#";
    val RML_NS = "http://semweb.mmlab.be/ns/rml#";
    val QL_NS = "http://semweb.mmlab.be/ns/ql#";

    // TriplesMap
    val xR2RML_LOGICALSOURCE_URI = xR2RML_NS + "logicalSource";
    val xR2RML_LOGICALSOURCE_PROPERTY = ResourceFactory.createProperty(xR2RML_LOGICALSOURCE_URI);

    // Logical source
    val xR2RML_QUERY_URI = xR2RML_NS + "query";
    val xR2RML_QUERY_PROPERTY = ResourceFactory.createProperty(xR2RML_QUERY_URI);

    val xR2RML_REFFORMULATION_URI = xR2RML_NS + "referenceFormulation";
    val xR2RML_REFFORMULATION_PROPERTY = ResourceFactory.createProperty(xR2RML_REFFORMULATION_URI);

    val RML_ITERATOR_URI = RML_NS + "iterator";
    val RML_ITERATOR_PROPERTY = ResourceFactory.createProperty(RML_ITERATOR_URI);

    // Reference formulation
    val QL_JSONPATH_URI = QL_NS + "JSONPath";
    val QL_JSONPATH_CLASS = ResourceFactory.createResource(QL_JSONPATH_URI);

    val QL_XPATH_URI = QL_NS + "XPath";
    val QL_XPATH_CLASS = ResourceFactory.createResource(QL_XPATH_URI);

    val xR2RML_COLUMN_URI = xR2RML_NS + "Column";
    val xR2RML_COLUMN_CLASS = ResourceFactory.createResource(xR2RML_COLUMN_URI);

    //val xR2RML_RRXDEFAULTFORMAT_URI = xR2RML_NS + "defaultFormat";
    //val xR2RML_RRXDEFAULTFORMAT_CLASS = ResourceFactory.createResource(xR2RML_RRXDEFAULTFORMAT_URI);

    val xR2RML_AUTHZD_REFERENCE_FORMULATION = Set(xR2RML_COLUMN_URI, QL_JSONPATH_URI, QL_XPATH_URI)

    // TermMap
    val xR2RML_REFERENCE_URI = xR2RML_NS + "reference";
    val xR2RML_REFERENCE_PROPERTY = ResourceFactory.createProperty(xR2RML_REFERENCE_URI);

    // TermType
    val xR2RML_RDFLIST_URI = xR2RML_NS + "RdfList";
    val xR2RML_RDFLIST_CLASS = ResourceFactory.createResource(xR2RML_RDFLIST_URI);

    val xR2RML_RDFBAG_URI = xR2RML_NS + "RdfBag";
    val xR2RML_RDFBAG_CLASS = ResourceFactory.createResource(xR2RML_RDFBAG_URI);

    val xR2RML_RDFSEQ_URI = xR2RML_NS + "RdfSeq";
    val xR2RML_RDFSEQ_CLASS = ResourceFactory.createResource(xR2RML_RDFSEQ_URI);

    val xR2RML_RDFALT_URI = xR2RML_NS + "RdfAlt";
    val xR2RML_RDFALT_CLASS = ResourceFactory.createResource(xR2RML_RDFALT_URI);

    val xR2RML_TEMPLATE_PATTERN = "\\{\"*\\w+(\\s\\w+)*\\\"*}";

    // Nested Term Map
    val xR2RML_NESTEDTM_URI = xR2RML_NS + "nestedTermMap";
    val xR2RML_NESTEDTM_PROPERTY = ResourceFactory.createProperty(xR2RML_NESTEDTM_URI);
}
