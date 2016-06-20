package es.upm.fi.dia.oeg.morph.base

class RDFTermIRI(val v: Object) extends RDFTerm(v) {

    override val isIRI = true

    def getIRI: String = value.asInstanceOf[String]

    override def equals(a: Any): Boolean = {
        this.getClass == a.getClass && value == a.asInstanceOf[RDFTermIRI].value
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode()
    }
}