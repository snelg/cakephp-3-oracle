<?php
namespace Cake\Oracle\Statement;

use Yajra\Pdo\Oci8\Statement;

/**
 * Small additional implementations to Yajra\Pdo\Oci8\Statement
 */
class Oci8Statement extends Statement
{
    /**
     * {@inheritDoc}
     */
    public function __construct($sth, Oci8 $connection = null, array $options = [])
    {
        if (is_object($sth) && $sth instanceof Statement) {
            list($sth, $connection, $options) = $this->_getParentProperties($sth, ['sth', 'connection', 'options']);
        }
        parent::__construct($sth, $connection, $options);
    }

    /**
     * Yajra's Oci8 "prepare" function returns a Yajra Statement object. But
     * we want an instance of this extended class.
     * To avoid completely re-implementing that "prepare" function just to
     * change one line (and thus also requiring future maintenance), we need
     * to grab the private properties of the Statement object so that we can
     * create an object of this class.
     *
     * @param Statement $sth Yajra Statement object that we're going to re-implement
     * @param string $names Properties we want to extract from Statement
     * @return array The values of the extracted properties
     */
    protected function _getParentProperties(Statement $sth, $names)
    {
        $reflection = new \ReflectionObject($sth);
        $properties = [];
        foreach ($names as $name) {
            $property = $reflection->getProperty($name);
            $property->setAccessible(true);
            $properties[] = $property->getValue($sth);
        }
        return $properties;
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
        return $success;
    }
}
