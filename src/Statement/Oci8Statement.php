<?php
namespace Cake\Oracle\Statement;

use Yajra\Pdo\Oci8\Statement;

/**
 * Small additional implementations to Yajra\Pdo\Oci8\Statement
 */
class Oci8Statement extends Statement
{
    protected $preserveBindings = [];

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

	/* Yajra's Oci8 "prepare" function returns a Yajra Statement object. But
     * we want an instance of this extended class.
     * To avoid completely re-implementing that "prepare" function just to
     * change one line (and thus also requiring future maintenance), we need
     * to grab the private properties of the Statement object so that we can
     * create an object of this class.
	 */
	private function _getParentProperties(Statement $sth, $names)
	{
		$reflection = new \ReflectionObject($sth);
		$properties = [];
		foreach($names as $name) {
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
