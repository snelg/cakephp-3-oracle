<?php
namespace Cake\Oracle\Statement;

use Cake\Database\Statement\StatementDecorator;
use Cake\Database\Statement\BufferedStatement;
use Cake\Database\Statement\BufferResultsTrait;

/**
 * Statement class meant to be used by an Oracle driver
 *
 * @internal
 */
class OracleStatement extends StatementDecorator
{

    use BufferResultsTrait;

    public $queryString;

    /**
     * {@inheritDoc}
     *
     */
    public function execute($params = null)
    {
        if ($this->_statement instanceof BufferedStatement) {
            $this->_statement = $this->_statement->getInnerStatement();
        }

        if ($this->_bufferResults) {
            $this->_statement = new BufferedStatement($this->_statement, $this->_driver);
        }

        return $this->_statement->execute($params);
    }

    public function __get($property)
    {
        if ($property === 'queryString') {
            return empty($this->queryString) ? $this->_statement->queryString : $this->queryString;
        }
    }

    /**
     * Override default StatementDecorator, because yajra indexes anonymous
     * params starting at offset 0, not 1
     *
     * @param array $params list of values to be bound
     * @param array $types list of types to be used, keys should match those in $params
     * @return void
     */
    public function bind($params, $types)
    {
        if (empty($params)) {
            return;
        }

        $annonymousParams = is_int(key($params)) ? true : false;

        //This line shoud be the only difference from StatementDecorator:
        $offset = 0;

        foreach ($params as $index => $value) {
            $type = null;
            if (isset($types[$index])) {
                $type = $types[$index];
            }
            if ($annonymousParams) {
                $index += $offset;
            }
            $this->bindValue($index, $value, $type);
        }
    }
}
