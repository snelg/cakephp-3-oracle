<?php
namespace Cake\Oracle\Statement;

use yajra\Pdo\Oci8\Statement;

/**
 * Small additional implementations to yajra\Pdo\Oci8\Statement
 */
class Oci8Statement extends Statement
{
    protected $preserveBindings = [];

    /**
     * {@inheritDoc}
     */
    public function __construct($sth, Oci8 $pdoOci8 = null, array $options = [])
    {
        if (is_object($sth) && $sth instanceof Statement) {
            parent::__construct($sth->_sth, $sth->_pdoOci8, $sth->_options);
        } else {
            parent::__construct($sth, $pdoOci8, $options);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function closeCursor()
    {
        if (empty($this->_sth)) {
            return true;
        }
        $success = oci_free_statement($this->_sth);
        $this->_sth = null;
        $this->preserveBindings = [];
        return $success;
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue($parameter, $variable, $dataType = PDO::PARAM_STR)
    {
        /* PHP 7 apparently cleans up bound variables before we get back to
         * oci_execute. But oci_bind_by_name binds variables by reference.
         * We need to make sure the actual referenced $variable remains
         * active until later, so we just toss it onto a pile here
         */
        $this->preserveBindings[] = &$variable;
        return $this->bindParam($parameter, $variable, $dataType);
    }
}
