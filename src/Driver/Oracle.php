<?php
/**
 * Copyright 2015 Glen Sawyer

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @copyright 2015 Glen Sawyer
 * @license http://www.apache.org/licenses/LICENSE-2.0 Apache License 2.0
 */
namespace Cake\Oracle\Driver;

use Cake\Database\Driver;
use Cake\Database\Query;
use Cake\Database\Statement\PDOStatement;
use Cake\Oracle\Dialect\OracleDialectTrait;
use Cake\Oracle\Schema\OracleSchema;
use Cake\Oracle\Statement\Oci8Statement;
use Cake\Oracle\Statement\OracleStatement;
use PDO;
use ReflectionObject;
use Yajra\Pdo\Oci8;

class Oracle extends Driver
{
    use OracleDialectTrait;

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

    /**
     * {@inheritDoc}
     */
    public function enabled()
    {
        return function_exists('oci_connect');
    }

    /**
     * {@inheritDoc}
     *
     * @return \Cake\Oracle\Schema\OracleSchema
     */
    public function schemaDialect()
    {
        if (!$this->_schemaDialect) {
            $this->_schemaDialect = new OracleSchema($this);
        }
        return $this->_schemaDialect;
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     *
     * @return \Cake\Oracle\Statement\OracleStatement
     */
    public function prepare($query)
    {
        $this->connect();
        $isObject = ($query instanceof Query);
        $queryStringRaw = $isObject ? $query->sql() : $query;
        $queryString = $this->_fromDualIfy($queryStringRaw);
        $yajraStatement = $this->_connection->prepare($queryString);
        $oci8Statement = new Oci8Statement($yajraStatement); //Need to override some un-implemented methods in yajra Oci8 "Statement" class
        $statement = new OracleStatement(new PDOStatement($oci8Statement, $this), $this); //And now wrap in a Cake-ified, bufferable Statement
        $statement->queryString = $queryStringRaw; //Oci8PDO does not correctly set read-only $queryString property, so we have a manual override
        if ($isObject) {
            if ($query->isBufferedResultsEnabled() === false || $query->type() != 'select') {
                $statement->bufferResults(false);
            }
        }
        return $statement;
    }

    /**
     * Add "FROM DUAL" to SQL statements that are SELECT statements
     * with no FROM clause specified
     *
     * @param string $queryString query
     * @return string
     */
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

    /**
     * {@inheritDoc}
     */
    public function disableForeignKeySQL()
    {
        return $this->_foreignKeySQL('disable');
    }

    /**
     * {@inheritDoc}
     */
    public function enableForeignKeySQL()
    {
        return $this->_foreignKeySQL('enable');
    }

    /**
     * Get the SQL for enabling or disabling foreign keys
     *
     * @param string $enableDisable "enable" or "disable"
     * @return string
     */
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
                    execute immediate 'alter table " .
                    "{$startQuote}' || r.owner || '{$endQuote}." .
                    "{$startQuote}' || r.table_name || '{$endQuote} " .
                    "{$enableDisable} constraint " .
                    "{$startQuote}' || r.constraint_name || '{$endQuote}';
                end loop;
            end;";
    }

    /**
     * {@inheritDoc}
     */
    public function supportsDynamicConstraints()
    {
        return true;
    }

    /**
     * Returns last id generated for a table or sequence in database
     * Override info:
     * Yajra expects sequence name to be passed in, but Cake typically passes
     * in table name. Yajra already has logic to guess sequence name based on
     * last-inserted-table name ("{tablename}_id_seq") IF null is passed in,
     * so we'll take a peek at that "last inserted table name" private property
     * and null it out if needed
     *
     * @param string|null $sequence Sequence (NOT TABLE in Oracle) to get last insert value from
     * @param string|null $ignored Ignored in Oracle
     * @return string|int
     */
    public function lastInsertId($sequence = null, $ignored = null)
    {
        $this->connect();

        if (!empty($sequence) && !empty($this->_connection)) {
            $reflection = new ReflectionObject($this->_connection);
            $property = $reflection->getProperty('table');
            $property->setAccessible(true);
            $baseTable = $property->getValue($this->_connection);
            if ($baseTable == $sequence) {
                $sequence = null;
            }
        }

        return $this->_connection->lastInsertId($sequence);
    }
}
