package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData

class RDBR2RMLMappingDocument(
    override val triplesMaps: Iterable[RDBR2RMLTriplesMap],
    mappingDocumentPath: String,
    mappingDocumentPrefixMap: Map[String, String])
    
        extends R2RMLMappingDocument(triplesMaps, mappingDocumentPath, mappingDocumentPrefixMap) {

    var dbMetaData: Option[MorphDatabaseMetaData] = None;

    override def getClassMappingByPropertyURIs(propertyURIs: Iterable[String]): Iterable[RDBR2RMLTriplesMap] = {
        super.getClassMappingByPropertyURIs(propertyURIs).map { tm => tm.asInstanceOf[RDBR2RMLTriplesMap] }
    }

    override def getClassMappingsByClassURI(classURI: String): Iterable[RDBR2RMLTriplesMap] = {
        super.getClassMappingsByClassURI(classURI).map { tm => tm.asInstanceOf[RDBR2RMLTriplesMap] }
    }

    override def getPossibleRange(predicateURI: String, cm: R2RMLTriplesMap): Iterable[RDBR2RMLTriplesMap] = {
        super.getPossibleRange(predicateURI, cm).map { tm => tm.asInstanceOf[RDBR2RMLTriplesMap] }
    }

    override def getPossibleRange(pm: R2RMLPredicateObjectMap): Iterable[RDBR2RMLTriplesMap] = {
        super.getPossibleRange(pm).map { tm => tm.asInstanceOf[RDBR2RMLTriplesMap] }
    }

    override def getPossibleRange(predicateURI: String): Iterable[RDBR2RMLTriplesMap] = {
        super.getPossibleRange(predicateURI).map { tm => tm.asInstanceOf[RDBR2RMLTriplesMap] }
    }
}