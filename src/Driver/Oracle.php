<?php
namespace Cake\Oracle\Driver;

use Cake\Oracle\Statement\OracleStatement;
use Cake\Oracle\Statement\Oci8Statement;
use Cake\Oracle\Schema\OracleSchema;
use Cake\Oracle\Dialect\OracleDialectTrait;
use Cake\Database\Driver;
use Cake\Database\Driver\PDODriverTrait;
use Cake\Database\Statement\PDOStatement;
use Cake\ORM\Query;
use yajra\Pdo\Oci8;
use PDO;

class Oracle extends Driver
{
    use OracleDialectTrait;
    use PDODriverTrait;

    protected $_baseConfig = [
        'flags' => [],
        'init' => []];
    /**
     * Establishes a connection to the database server
     *
     * @param string $dsn A Driver-specific PDO-DSN
     * @param array $config configuration to be used for creating connection
     * @return bool true on success
     */
    protected function _connect($dsn, array $config)
    {
        $connection = new Oci8(
            $dsn,
            $config['username'],
            $config['password'],
            $config['flags']
        );
        $this->connection($connection);
        return true;
    }

    public function enabled()
    {
        return function_exists('oci_connect');
    }

    public function schemaDialect()
    {
        if (!$this->_schemaDialect) {
            $this->_schemaDialect = new OracleSchema($this);
        }
        return $this->_schemaDialect;
    }

    public function connect()
    {
        if ($this->_connection) {
            return true;
        }
        $config = $this->_config;

        $config['init'][] = "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS'";

        $config['flags'] += [
            PDO::ATTR_PERSISTENT => empty($config['persistent']) ? false : $config['persistent'],
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_ORACLE_NULLS => true,
            PDO::NULL_EMPTY_STRING => true
        ];

        $this->_connect($config['database'], $config);

        if (!empty($config['init'])) {
            foreach ((array)$config['init'] as $command) {
                $this->connection()->exec($command);
            }
        }
        return true;
    }

    public function prepare($query)
    {
        $this->connect();
        $isObject = ($query instanceof Query) || ($query instanceof \Cake\Database\Query);
        $queryStringRaw = $isObject ? $query->sql() : $query;
        $queryString = $this->_fromDualIfy($queryStringRaw);
        $yajraStatement = $this->_connection->prepare($queryString);
        $oci8Statement = new Oci8Statement($yajraStatement); //Need to override some un-implemented methods in yajra Oci8 "Statement" class
        $statement = new OracleStatement(new PDOStatement($oci8Statement, $this), $this); //And now wrap in a Cake-ified, bufferable Statement
        $statement->queryString = $queryStringRaw; //Oci8PDO does not correctly set read-only $queryString property, so we have a manual override
        if ($isObject && $query->bufferResults() === false) {
            $statement->bufferResults(false);
        }
        return $statement;
    }

    protected function _fromDualIfy($queryString)
    {
        $statement = strtolower(trim($queryString));
        if (strpos($statement, 'select') !== 0 || preg_match('/ from /', $statement)) {
            return $queryString;
        }
        //Doing a SELECT without a FROM (e.g. "SELECT 1 + 1") does not work in Oracle:
        //need to have "FROM DUAL" at the end
        return "{$queryString} FROM DUAL";
    }

    public function disableForeignKeySQL()
    {
        return $this->_foreignKeySQL('disable');
    }

    public function enableForeignKeySQL()
    {
        return $this->_foreignKeySQL('enable');
    }

    protected function _foreignKeySQL($enableDisable)
    {
        $startQuote = $this->_startQuote;
        $endQuote = $this->_endQuote;
        if (!empty($this->_config['schema'])) {
            $schemaName = strtoupper($this->_config['schema']);
            $fromWhere = "from sys.all_constraints
                where owner = '{$schemaName}' and constraint_type = 'R'";
        } else {
            $fromWhere = "from sys.user_constraints
                where constraint_type = 'R'";
        }
        return "declare
            cursor c is select owner, table_name, constraint_name
                {$fromWhere};
            begin
                for r in c loop
                    execute immediate 'alter table "
                    . "{$startQuote}' || r.owner || '{$endQuote}."
                    . "{$startQuote}' || r.table_name || '{$endQuote} "
                    . "{$enableDisable} constraint "
                    . "{$startQuote}' || r.constraint_name || '{$endQuote}';
                end loop;
            end;";
    }

    public function supportsDynamicConstraints()
    {
        return true;
    }
}
