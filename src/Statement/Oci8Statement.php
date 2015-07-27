<?php
namespace Cake\Oracle\Statement;

use yajra\Pdo\Oci8\Statement;

/**
 * Small additional implementations to yajra\Pdo\Oci8\Statement
 */
class Oci8Statement extends Statement
{
    /**
     * Constructor
     *
     */
    public function __construct($sth, Oci8 $pdoOci8 = null, array $options = array())
    {
        if (is_object($sth) && $sth instanceof Statement) {
            parent::__construct($sth->_sth, $sth->_pdoOci8, $sth->_options);
        } else {
            parent::__construct($sth, $pdoOci8, $options);
        }
    }

    public function closeCursor()
    {
        if (empty($this->_sth)) {
            return true;
        }
        $success = oci_free_statement($this->_sth);
        $this->_sth = null;
        return $success;
    }
}