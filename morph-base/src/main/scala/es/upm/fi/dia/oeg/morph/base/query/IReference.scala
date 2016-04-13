package es.upm.fi.dia.oeg.morph.base.query

/**
 * Among the conditions of the <i>Where</i> part of an atomic abstract query,
 * this trait denotes those that have a reference field, i.e. Equals, IsNull and IsnotNull.
 * 
 * @author Franck Michel, I3S laboratory
 */
trait IReference {
    var reference: String

    def hasReference = true
}