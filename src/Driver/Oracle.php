<?php
namespace Cake\Oracle\Driver;

use Cake\Oracle\Statement\OracleStatement;
use Cake\Oracle\Statement\Oci8Statement;
use Cake\Oracle\OracleCompiler;
use Cake\Oracle\Schema\OracleSchema;
use Cake\Oracle\Dialect\OracleDialectTrait;
use Cake\Database\Driver;
use Cake\Database\Driver\PDODriverTrait;
use Cake\Database\Statement\PDOStatement;
use Cake\ORM\Query;
use yajra\Pdo\Oci8;
use yajra\Pdo\Oci8\Exceptions\Oci8Exception;
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

        $config['init'][] = "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'";

        $config['flags'] += [
            PDO::ATTR_PERSISTENT => empty($config['persistent']) ? false : $config['persistent'],
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
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
        $isObject = $query instanceof Query;
        $queryString = $isObject ? $query->sql() : $query;
        $yajraStatement = $this->_connection->prepare($queryString);
        $oci8Statement = new Oci8Statement($yajraStatement); //Need to override some un-implemented methods in yajra Oci8 "Statement" class
        $statement = new OracleStatement(new PDOStatement($oci8Statement, $this), $this); //And now wrap in a Cake-ified, bufferable Statement
        $statement->queryString = $queryString; //Oci8PDO does not correctly set read-only $queryString property, so we have a manual override
        if ($isObject && $query->bufferResults() === false) {
            $statement->bufferResults(false);
        }
        return $statement;
    }

    public function newCompiler()
    {
        return new OracleCompiler;
    }

    public function disableForeignKeySQL()
    {
        throw new Oci8Exception("disableForeignKeySQL has not been implemented");
    }

    public function enableForeignKeySQL()
    {
        throw new Oci8Exception("enableForeignKeySQL has not been implemented");
    }

}
