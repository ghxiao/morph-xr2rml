package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import scala.collection.JavaConversions._
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import java.util.HashMap
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import com.hp.hpl.jena.rdf.model.Statement
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath

abstract class R2RMLTermMap(
    val termMapType: Constants.MorphTermMapType.Value,
    val termType: Option[String],
    val datatype: Option[String],
    val languageTag: Option[String],
    val nestedTermMap: Option[xR2RMLNestedTermMap],
    /** Reference formulation from the logical source */
    val refFormulaion: String)

        extends MorphR2RMLElement with IConstantTermMap with IColumnTermMap with ITemplateTermMap with IReferenceTermMap {

    /** Jena resource corresponding to that term map */
    var rdfNode: RDFNode = null;

    val logger = Logger.getLogger(this.getClass().getName());

    def this(rdfNode: RDFNode, refForm: String) = {
        this(R2RMLTermMap.extractTermMapType(rdfNode),
            R2RMLTermMap.extractTermType(rdfNode),
            R2RMLTermMap.extractDatatype(rdfNode),
            R2RMLTermMap.extractLanguageTag(rdfNode),
            R2RMLTermMap.extractNestedTermMap(rdfNode),
            refForm);
        this.rdfNode = rdfNode;
        this.parse(rdfNode);
    }

    def accept(visitor: MorphR2RMLElementVisitor): Object = {
        visitor.visit(this);
    }

    /**
     * Decide the type of the term map (constant, column, reference, template) based on its properties,
     * and assign the value of the appropriate trait member: IConstantTermMap.constantValue, IColumnTermMap.columnName etc.
     *
     * If the term map resource has no property (constant, column, reference, template) then it means that this is a
     * constant term map like "[] rr:predicate ex:name".
     *
     * If the node passed in not a resource but a literal, then it means that we have a constant property wich object
     * is a literal and not an IRI or blank node, like in: "[] rr:object 'any string';"
     *
     * @param rdfNode the term map Jena resource
     */
    def parse(rdfNode: RDFNode) = {
        this.rdfNode = rdfNode;

        if (rdfNode.isLiteral) {
            // We are in the case of a constant property with a literal object, like "[] rr:object 'NAME'",
            this.constantValue = rdfNode.toString()
        } else {
            val resourceNode = rdfNode.asResource();

            val constantStatement = resourceNode.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
            if (constantStatement != null)
                this.constantValue = constantStatement.getObject().toString();
            else {
                val columnStatement = resourceNode.getProperty(Constants.R2RML_COLUMN_PROPERTY);
                if (columnStatement != null)
                    this.columnName = columnStatement.getObject().toString();
                else {
                    val templateStatement = resourceNode.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
                    if (templateStatement != null)
                        this.templateString = templateStatement.getObject().toString();
                    else {
                        val refStmt = resourceNode.getProperty(xR2RML_Constants.xR2RML_REFERENCE_PROPERTY);
                        if (refStmt != null)
                            this.reference = refStmt.getObject().toString();
                        else {
                            // We are in the case of a constant property, like "[] rr:predicate ex:name",
                            this.constantValue = rdfNode.toString()
                        }
                    }
                }
            }
        }
    }

    /**
     * Return the term type mentioned by property rr:termType or the default term type otherwise
     */
    def inferTermType(): String = {
        if (this.termType.isDefined) {
            this.termType.get
        } else {
            this.getDefaultTermType
        }
    }

    def getDefaultTermType(): String = {
        val result = this match {
            case _: R2RMLObjectMap => {
                if (termMapType == Constants.MorphTermMapType.ColumnTermMap
                    || termMapType == Constants.MorphTermMapType.ReferenceTermMap
                    || languageTag.isDefined
                    || datatype.isDefined) {
                    Constants.R2RML_LITERAL_URI;
                } else
                    Constants.R2RML_IRI_URI;
            }

            case _: R2RMLPredicateMap => {
                if (termType.isDefined && !termType.get.equals(Constants.R2RML_IRI_URI)) {
                    throw new Exception("Illegal termtype in predicateMap: " + termType.get);
                } else
                    Constants.R2RML_IRI_URI;
            }

            case _: R2RMLSubjectMap => { Constants.R2RML_IRI_URI; }
            case _ => { Constants.R2RML_IRI_URI; }
        }
        result;
    }

    /**
     * Return the list of column names (strings) referenced by the term map.
     * Nil in case of a constant-valued term-map, a list of one string for a column-valued term map,
     * and a list of several strings for a template-valued term map.
     *
     * @return list of strings representing column names. Cannot return null
     */
    def getReferencedColumns(): List[String] = {
        val result = this.termMapType match {
            case Constants.MorphTermMapType.ConstantTermMap => { Nil }
            case Constants.MorphTermMapType.ColumnTermMap => { List(this.columnName) }
            case Constants.MorphTermMapType.ReferenceTermMap => {
                // Parse reference as a mixed syntax path and return the column referenced in the first path "Column()" 
                List(MixedSyntaxPath(this.reference, refFormulaion).getReferencedColumn.get)
            }
            case Constants.MorphTermMapType.TemplateTermMap => {
                // Get the list of template strings
                val tplStrings = RegexUtility.getTemplateColumns(this.templateString)
                
                // For each one, parse it as a mixed syntax path and return the column referenced in the first path "Column()" 
                tplStrings.map(tplString => MixedSyntaxPath(tplString, refFormulaion).getReferencedColumn.get)
            }
            case _ => { throw new Exception("Invalid term map type") }
        }
        result
    }

    def getOriginalValue(): String = {
        val result = this.termMapType match {
            case Constants.MorphTermMapType.ConstantTermMap => { this.constantValue; }
            case Constants.MorphTermMapType.ColumnTermMap => { this.columnName; }
            case Constants.MorphTermMapType.TemplateTermMap => { this.templateString; }
            case Constants.MorphTermMapType.ReferenceTermMap => { this.reference; }
            case _ => { null; }
        }
        result
    }

    def isBlankNode(): Boolean = {
        Constants.R2RML_BLANKNODE_URI.equals(this.termType)
    }

    override def toString(): String = {
        var result = this.termMapType match {
            case Constants.MorphTermMapType.ConstantTermMap => { "rr:constant"; }
            case Constants.MorphTermMapType.ColumnTermMap => { "rr:column"; }
            case Constants.MorphTermMapType.TemplateTermMap => { "rr:template"; }
            case Constants.MorphTermMapType.ReferenceTermMap => { "xrr:reference"; }
            case _ => "";
        }
        result += "::" + this.getOriginalValue();
        return result;
    }
}

object R2RMLTermMap {
    val logger = Logger.getLogger(this.getClass().getName());
    def determineTermMapType(resource: Resource) = {}

    /**
     * Extract the value of the rr:termpType property, returns None is no property found
     * => means that it shall be deduced
     */
    def extractTermType(rdfNode: RDFNode) = {
        rdfNode match {
            case resource: Resource => {
                val termTypeStatement = resource.getProperty(Constants.R2RML_TERMTYPE_PROPERTY);
                if (termTypeStatement != null) {
                    Some(termTypeStatement.getObject().toString());
                } else
                    None
            }
            case _ => None
        }
    }

    /**
     * Deduces the type of the term map (constant, column, reference, template) based on its properties
     * @param rdfNode the term map node
     * @throws exception in case the term map type cannot be decided
     */
    def extractTermMapType(rdfNode: RDFNode) = {
        rdfNode match {
            case resource: Resource => {
                if (resource.getProperty(Constants.R2RML_CONSTANT_PROPERTY) != null)
                    Constants.MorphTermMapType.ConstantTermMap;
                else if (resource.getProperty(Constants.R2RML_COLUMN_PROPERTY) != null)
                    Constants.MorphTermMapType.ColumnTermMap;
                else if (resource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY) != null)
                    Constants.MorphTermMapType.TemplateTermMap;
                else if (resource.getProperty(xR2RML_Constants.xR2RML_REFERENCE_PROPERTY) != null)
                    Constants.MorphTermMapType.ReferenceTermMap;
                else {
                    val errorMessage = "Invalid term map " + resource.getLocalName() + ". Should have one of rr:constant, rr:column, rr:template or xrr:reference.";
                    logger.error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }
            case _ => {
                Constants.MorphTermMapType.ConstantTermMap;
            }
        }
    }

    def extractDatatype(rdfNode: RDFNode) = {
        rdfNode match {
            case resource: Resource => {
                val datatypeStatement = resource.getProperty(Constants.R2RML_DATATYPE_PROPERTY);
                if (datatypeStatement != null) {
                    Some(datatypeStatement.getObject().toString());
                } else
                    None
            }
            case _ => None
        }
    }

    def extractLanguageTag(rdfNode: RDFNode) = {
        rdfNode match {
            case resource: Resource => {
                val languageStatement = resource.getProperty(Constants.R2RML_LANGUAGE_PROPERTY);
                if (languageStatement != null) {
                    Some(languageStatement.getObject().toString());
                } else
                    None
            }
            case _ => None
        }
    }

    def extractNestedTermMap(rdfNode: RDFNode): Option[xR2RMLNestedTermMap] = {
        None

        //        var recurs = rdfNode match {
        //            case resource: Resource => {
        //                var hasParse: Statement = null
        //                try {
        //                    hasParse = resource.getProperty(xR2RML_Constants.xR2RML_PARSE_PROPERTY)
        //                } catch { case e: Exception => logger.error(" No more property \"parse\" "); }
        //
        //                if (hasParse == null) { null }
        //                else {
        //                    // if it has a parse
        //                    var termt: String = null;
        //                    var parset: String = null;
        //                    var language: String = null;
        //                    var datatype: String = null;
        //                    try {
        //                        language = hasParse.getProperty(Constants.R2RML_LANGUAGE_PROPERTY).getObject().toString();
        //                        datatype = hasParse.getProperty(Constants.R2RML_DATATYPE_PROPERTY).getObject().toString();
        //                    } catch { case e: Exception => logger.error(" Missing property datatype or languague "); }
        //
        //                    try {
        //                        termt = hasParse.getProperty(Constants.R2RML_TERMTYPE_PROPERTY).getObject().toString();
        //                        parset = hasParse.getProperty(xR2RML_Constants.xR2RML_PARSETYPE_PROPERTY).getObject().toString();
        //                    } catch { case e: Exception => logger.error(" Missing property in property \"parse\" "); System.exit(0); }
        //                    if (termt == null) { logger.error("parse property without termType property"); System.exit(0); }
        //                    if (parset == null) { logger.error("parse property without parseType property"); System.exit(0) }
        //
        //                    if (parset == xR2RML_Constants.xR2RML_RRXLISTORMAP_URI) {
        //
        //                        if (termt == xR2RML_Constants.xR2RML_RDFALT_URI || termt == xR2RML_Constants.xR2RML_RDFBAG_URI
        //                            || termt == xR2RML_Constants.xR2RML_RDFSEQ_URI || termt == xR2RML_Constants.xR2RML_RDFLIST_URI) {
        //                            var recursive_parse = new xR2RMLRecursiveParse(
        //                                Some(hasParse.getProperty(Constants.R2RML_TERMTYPE_PROPERTY).getObject().toString()),
        //                                Some(hasParse.getProperty(xR2RML_Constants.xR2RML_PARSETYPE_PROPERTY).getObject().toString()),
        //                                extractRecursiveParse(resource.getProperty(xR2RML_Constants.xR2RML_PARSE_PROPERTY).getObject()),
        //                                Some(datatype), Some(language))
        //                            recursive_parse
        //                        } else {
        //                            logger.error("non concordance between propeties in  \"parse\" ");
        //                            System.exit(0);
        //                            null
        //                        }
        //                    } else if (parset == Constants.R2RML_LITERAL_URI) {
        //
        //                        if (termt == Constants.R2RML_BLANKNODE_URI || termt == Constants.R2RML_LITERAL_URI || termt == Constants.R2RML_IRI_URI
        //                            || termt == xR2RML_Constants.xR2RML_RDFALT_URI || termt == xR2RML_Constants.xR2RML_RDFBAG_URI
        //                            || termt == xR2RML_Constants.xR2RML_RDFSEQ_URI || termt == xR2RML_Constants.xR2RML_RDFLIST_URI) {
        //                            var recursive_parse = new xR2RMLRecursiveParse(
        //                                Some(hasParse.getProperty(Constants.R2RML_TERMTYPE_PROPERTY).getObject().toString()),
        //                                Some(hasParse.getProperty(xR2RML_Constants.xR2RML_PARSETYPE_PROPERTY).getObject().toString()),
        //                                extractRecursiveParse(resource.getProperty(xR2RML_Constants.xR2RML_PARSE_PROPERTY).getObject()),
        //                                Some(datatype), Some(language))
        //                            recursive_parse
        //                        } else {
        //                            logger.error("non concordance between propeties in  \"parse\" ");
        //                            System.exit(0);
        //                            null
        //                        }
        //                    } else {
        //                        logger.error("non concordance between propeties in  \"parse\" ");
        //                        System.exit(0);
        //                        null
        //                    }
        //                    //   }
        //                }
        //            }
        //            case _ => {
        //                null
        //            }
        //        }
        //
        //        recurs
    }
    /////////////////////////////////////////////// 

    /**
     * From a term map, return a list with the following elements:
     * - type of term map (constant, column, reference, template),
     * - term type (as a Jena resource)
     * - optional data type
     * - optional language tag
     * - optional nested term map
     */
    def extractCoreProperties(rdfNode: RDFNode) = {
        val termMapType = R2RMLTermMap.extractTermMapType(rdfNode);
        val termType = R2RMLTermMap.extractTermType(rdfNode);
        val datatype = R2RMLTermMap.extractDatatype(rdfNode);
        val languageTag = R2RMLTermMap.extractLanguageTag(rdfNode);
        val nestedTM = R2RMLTermMap.extractNestedTermMap(rdfNode);

        logger.trace("Extracted term map core properties: termMapType: " + termMapType + ". termType: "
            + termType + ". datatype: " + datatype + ". languageTag: " + languageTag
            + ". nestedTermMap: " + nestedTM)

        val coreProperties = (termMapType, termType, datatype, languageTag, nestedTM)
        coreProperties;
    }

    /**
     * Find term maps of the given triples map by term position (subject, object, predicate) by looking for
     * constant (e.g.g rr:subject) and non-constant (e.g. rr:subjectMap) properties
     *
     * @param resource instance of R2RMLTriplesMap
     * @param termPos what to extract: subject, predicate, object or graph
     * @param refFormulation the reference formulation of the current triples map's logical source
     */
    def extractTermMaps(resource: Resource, termPos: Constants.MorphPOS.Value, refFormulation: String): Set[R2RMLTermMap] = {

        // Extract term map introduced with non-constant properties rr:subjectMap, rr:predicateMap, rr:objectMap and rr:graphMap
        val nonConstProperties = termPos match {
            case Constants.MorphPOS.sub => Constants.R2RML_SUBJECTMAP_PROPERTY
            case Constants.MorphPOS.pre => Constants.R2RML_PREDICATEMAP_PROPERTY
            case Constants.MorphPOS.obj => Constants.R2RML_OBJECTMAP_PROPERTY
            case Constants.MorphPOS.graph => Constants.R2RML_GRAPHMAP_PROPERTY
        }
        var stmts = resource.listProperties(nonConstProperties);
        val maps1 =
            if (stmts != null) {
                stmts.toList().flatMap(mapStatement => {

                    val stmtObject = mapStatement.getObject();
                    termPos match {
                        case Constants.MorphPOS.sub => Some(R2RMLSubjectMap(stmtObject, refFormulation))
                        case Constants.MorphPOS.pre => Some(R2RMLPredicateMap(stmtObject, refFormulation))
                        case Constants.MorphPOS.obj => {
                            val stmtObjectResource = stmtObject.asInstanceOf[Resource]
                            if (!R2RMLRefObjectMap.isRefObjectMap(stmtObjectResource)) {
                                Some(R2RMLObjectMap(stmtObject, refFormulation))
                            } else None
                        }
                        case Constants.MorphPOS.graph => {
                            val gm = R2RMLGraphMap(stmtObject, refFormulation);
                            if (Constants.R2RML_DEFAULT_GRAPH_URI.equals(gm.getOriginalValue)) {
                                None
                            } else { Some(gm) }
                        }
                        case _ => { None }
                    }
                })
            } else { Nil }

        // Extract term map introduced with constant properties rr:subject, rr:predicate, rr:object and rr:graph
        val constProperties = termPos match {
            case Constants.MorphPOS.sub => Constants.R2RML_SUBJECT_PROPERTY
            case Constants.MorphPOS.pre => Constants.R2RML_PREDICATE_PROPERTY
            case Constants.MorphPOS.obj => Constants.R2RML_OBJECT_PROPERTY
            case Constants.MorphPOS.graph => Constants.R2RML_GRAPH_PROPERTY
        }
        stmts = resource.listProperties(constProperties)
        val maps2 =
            if (stmts != null) {
                stmts.toList().flatMap(mapStatement => {
                    val stmtObject = mapStatement.getObject()
                    termPos match {
                        case Constants.MorphPOS.sub => {
                            val sm = new R2RMLSubjectMap(Constants.MorphTermMapType.ConstantTermMap, Some(Constants.R2RML_IRI_URI), None, None, Set.empty, Set.empty, refFormulation);
                            sm.parse(stmtObject)
                            Some(sm)
                        }
                        case Constants.MorphPOS.pre => {
                            val pm = new R2RMLPredicateMap(Constants.MorphTermMapType.ConstantTermMap, Some(Constants.R2RML_IRI_URI), None, None, refFormulation);
                            pm.parse(stmtObject)
                            Some(pm);
                        }
                        case Constants.MorphPOS.obj => {
                            val om = new R2RMLObjectMap(Constants.MorphTermMapType.ConstantTermMap, Some(Constants.R2RML_LITERAL_URI), None, None, None, refFormulation);
                            om.parse(stmtObject)
                            Some(om)
                        }
                        case Constants.MorphPOS.graph => {
                            val gm = new R2RMLGraphMap(Constants.MorphTermMapType.ConstantTermMap, Some(Constants.R2RML_IRI_URI), None, None, refFormulation);
                            gm.parse(stmtObject)
                            if (Constants.R2RML_DEFAULT_GRAPH_URI.equals(gm.getOriginalValue)) {
                                None
                            } else { Some(gm) }
                        }
                        case _ => { throw new Exception("Invalid triple term position: " + termPos) }
                    }
                })
            } else { Nil }

        val maps = maps1 ++ maps2
        val mapsSet = maps.toSet
        logger.trace("Extracted term maps: " + mapsSet)
        mapsSet
    }

}