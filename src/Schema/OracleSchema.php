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
namespace Cake\Oracle\Schema;

use Cake\Database\Schema\BaseSchema;
use Cake\Database\Schema\TableSchema;
use Yajra\Pdo\Oci8\Exceptions\Oci8Exception;

/**
 * Schema generation/reflection features for Oracle
 */
class OracleSchema extends BaseSchema
{
    /**
     * {@inheritDoc}
     */
    public function describeColumnSql($tableName, $config)
    {
        list($table, $schema) = $this->_tableSplit($tableName, $config);
        if (empty($schema)) {
            return [
                'SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT ' .
                'FROM user_tab_columns WHERE table_name = :bindTable', [
                    ':bindTable' => $table]];
        }
        return [
            'SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT ' .
            'FROM all_tab_columns WHERE table_name = :bindTable AND owner = :bindOwner', [
                ':bindTable' => $table,
                ':bindOwner' => $schema]];
    }

    /**
     * {@inheritDoc}
     */
    public function describeIndexSql($tableName, $config)
    {
        list($table, $schema) = $this->_tableSplit($tableName, $config);
        if (empty($schema)) {
            return [
                'SELECT cc.table_name, cc.column_name, cc.constraint_name, c.constraint_type, i.index_name, i.uniqueness ' .
                'FROM user_cons_columns cc ' .
                'LEFT JOIN user_indexes i ON(i.index_name  = cc.constraint_name) ' .
                'LEFT JOIN user_constraints c ON(c.constraint_name = cc.constraint_name) ' .
                'WHERE cc.table_name = :bindTable', [
                    ':bindTable' => $table]];
        }
        return [
            'SELECT cc.table_name, cc.column_name, cc.constraint_name, c.constraint_type, i.index_name, i.uniqueness ' .
            'FROM all_cons_columns cc ' .
            'LEFT JOIN all_indexes i ON(i.index_name  = cc.constraint_name AND cc.owner = i.owner) ' .
            'LEFT JOIN all_constraints c ON(c.constraint_name = cc.constraint_name AND c.owner = cc.owner) ' .
            'WHERE cc.table_name = :bindTable ' .
            'AND cc.owner = :bindOwner', [
                ':bindTable' => $table,
                ':bindOwner' => $schema]];
    }

    /**
     * {@inheritDoc}
     */
    public function describeForeignKeySql($tableName, $config)
    {
        list($table, $schema) = $this->_tableSplit($tableName, $config);
        if (empty($schema)) {
            return [
                'SELECT cc.column_name, cc.constraint_name, r.owner AS REFERENCED_OWNER, r.table_name AS REFERENCED_TABLE_NAME, r.column_name AS REFERENCED_COLUMN_NAME, c.delete_rule ' .
                'FROM user_cons_columns cc ' .
                'JOIN user_constraints c ON(c.constraint_name = cc.constraint_name) ' .
                'JOIN user_cons_columns r ON(r.constraint_name = c.r_constraint_name) ' .
                "WHERE c.constraint_type = 'R' " .
                'AND cc.table_name = :bindTable', [
                    ':bindTable' => $table]];
        }
        return [
            'SELECT cc.column_name, cc.constraint_name, r.owner AS REFERENCED_OWNER, r.table_name AS REFERENCED_TABLE_NAME, r.column_name AS REFERENCED_COLUMN_NAME, c.delete_rule ' .
            'FROM all_cons_columns cc ' .
            'JOIN all_constraints c ON(c.constraint_name = cc.constraint_name AND c.owner = cc.owner) ' .
            'JOIN all_cons_columns r ON(r.constraint_name = c.r_constraint_name AND r.owner = c.r_owner) ' .
            "WHERE c.constraint_type = 'R' " .
            'AND cc.table_name = :bindTable ' .
            'AND cc.owner = :bindOwner', [
                ':bindTable' => $table,
                ':bindOwner' => $schema]];
    }

    /**
     * {@inheritDoc}
     */
    public function convertColumnDescription(TableSchema $table, $row)
    {
        switch ($row['DATA_TYPE']) {
            case 'DATE':
                $field = ['type' => 'datetime', 'length' => null];
                break;
            case 'TIMESTAMP':
            case 'TIMESTAMP(6)':
            case 'TIMESTAMP(9)':
                $field = ['type' => 'timestamp', 'length' => null];
                break;
            case 'NUMBER':
                if ($row['DATA_PRECISION'] == null) {
                    $field = ['type' => 'decimal', 'length' => $row['DATA_LENGTH']];
                } elseif ($row['DATA_PRECISION'] == 1) {
                    $field = ['type' => 'boolean', 'length' => null];
                } else {
                    if ($row['DATA_SCALE'] > 0) {
                        $field = ['type' => 'decimal', 'length' => $row['DATA_PRECISION'], 'precision' => $row['DATA_SCALE']];
                    } else {
                        $field = ['type' => 'integer', 'length' => $row['DATA_PRECISION']];
                    }
                }
                break;
            case 'FLOAT':
                $field = ['type' => 'decimal', 'length' => $row['DATA_PRECISION']];
                break;
            case 'CHAR':
            case 'VARCHAR2':
            case 'NVARCHAR2':
                $field = ['type' => 'string', 'length' => $row['DATA_LENGTH']];
                break;
            case 'LONG':
                $field = ['type' => 'string', 'length' => null];
                break;
            case 'LONG RAW':
                $field = ['type' => 'binary', 'length' => null];
                break;
            case 'CLOB':
                $field = ['type' => 'string', 'length' => $row['DATA_LENGTH']];
                break;
            case 'RAW':
            case 'BLOB':
                $field = ['type' => 'binary', 'length' => $row['DATA_LENGTH']];
                break;
            default:
        }
        $field += [
            'null' => $row['NULLABLE'] === 'Y' ? true : false,
            'default' => $row['DATA_DEFAULT']];
        $table->addColumn(strtolower($row['COLUMN_NAME']), $field);
    }

    /**
     * {@inheritDoc}
     */
    public function convertIndexDescription(TableSchema $table, $row)
    {
        $type = null;
        $columns = $length = [];

        $name = $row['CONSTRAINT_NAME'];
        switch ($row['CONSTRAINT_TYPE']) {
            case 'P':
                $name = $type = TableSchema::CONSTRAINT_PRIMARY;
                break;
            case 'U':
                $type = TableSchema::CONSTRAINT_UNIQUE;
                break;
            default:
                return; //Not doing anything here with Oracle "Check" constraints or "Reference" constraints
        }

        $columns[] = strtolower($row['COLUMN_NAME']);

        $isIndex = (
            $type === TableSchema::INDEX_INDEX ||
            $type === TableSchema::INDEX_FULLTEXT
        );
        if ($isIndex) {
            $existing = $table->index($name);
        } else {
            $existing = $table->getConstraint($name);
        }

        if (!empty($existing)) {
            $columns = array_merge($existing['columns'], $columns);
            $length = array_merge($existing['length'], $length);
        }
        if ($isIndex) {
            $table->addIndex($name, [
                'type' => $type,
                'columns' => $columns,
                'length' => $length
            ]);
        } else {
            $table->addConstraint($name, [
                'type' => $type,
                'columns' => $columns,
                'length' => $length
            ]);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function convertForeignKeyDescription(TableSchema $table, $row)
    {
        $data = [
            'type' => TableSchema::CONSTRAINT_FOREIGN,
            'columns' => [strtolower($row['COLUMN_NAME'])],
            'references' => ["{$row['REFERENCED_OWNER']}.{$row['REFERENCED_TABLE_NAME']}", strtolower($row['REFERENCED_COLUMN_NAME'])],
            'update' => TableSchema::ACTION_SET_NULL,
            'delete' => $this->_convertOnClause($row['DELETE_RULE']),
        ];
        $name = $row['CONSTRAINT_NAME'];
        $table->addConstraint($name, $data);
    }

    /**
     * Helper method for generating key SQL snippets.
     *
     * @param string $tableName Table name, possibly including schema
     * @param array $config The connection configuration to use for
     *    getting tables from.
     * @return string
     */
    protected function _tableSplit($tableName, $config)
    {
        $schema = null;
        $table = strtoupper($tableName);
        if (strpos($tableName, '.') !== false) {
            $tableSplit = explode('.', $tableName);
            $table = strtoupper($tableSplit[1]);
            $schema = strtoupper($tableSplit[0]);
        } elseif (!empty($config['schema'])) {
            $schema = strtoupper($config['schema']);
        }
        return [$table, $schema];
    }

    /**
     * {@inheritDoc}
     */
    public function columnSql(TableSchema $table, $name)
    {
        $data = $table->column($name);
        if ($this->_driver->autoQuoting()) {
            $out = $this->_driver->quoteIdentifier($name);
        } else {
            $out = $name;
        }
        $typeMap = [
            'integer' => ' NUMBER',
            'biginteger' => ' NUMBER',
            'boolean' => ' NUMBER',
            'binary' => ' BLOB',
            'float' => ' FLOAT',
            'decimal' => ' NUMBER',
            'text' => ' CLOB',
            'date' => ' DATE',
            'time' => ' DATE',
            'datetime' => ' DATE',
            'timestamp' => ' TIMESTAMP(6)',
            'uuid' => ' VARCHAR2(36)',
        ];
        if (isset($typeMap[$data['type']])) {
            $out .= $typeMap[$data['type']];
        } else {
            switch ($data['type']) {
                case 'string':
                    $out .= !empty($data['fixed']) ? ' CHAR' : ' VARCHAR';
                    if (!isset($data['length'])) {
                        $data['length'] = 255;
                    }
                    break;
                default:
                    throw new Oci8Exception("Column type {$data['type']} not yet implemented");
            }
        }

        $hasLength = ['integer', 'string'];
        if (in_array($data['type'], $hasLength, true) && isset($data['length'])) {
            $out .= '(' . (int)$data['length'] . ')';
        }

        $hasPrecision = ['float', 'decimal'];
        if (in_array($data['type'], $hasPrecision, true) &&
            (isset($data['length']) || isset($data['precision']))
        ) {
            $out .= '(' . (int)$data['length'] . ',' . (int)$data['precision'] . ')';
        }

        if (isset($data['null']) && $data['null'] === false) {
            $out .= ' NOT NULL';
        }

        if (isset($data['default'])) {
            $out .= ' DEFAULT ' . $this->_driver->schemaValue($data['default']);
        }

        return $out;
    }

    /**
     * Helper method for generating key SQL snippets.
     *
     * @param string $prefix The key prefix
     * @param array $data Key data.
     * @return string
     */
    protected function _keySql($prefix, $data)
    {
        $columns = $data['columns'];
        if ($this->_driver->autoQuoting()) {
            $columns = array_map(
                [$this->_driver, 'quoteIdentifier'],
                $columns
            );
        }

        if ($data['type'] === TableSchema::CONSTRAINT_FOREIGN) {
            $keyName = $data['references'][0];
            if ($this->_driver->autoQuoting()) {
                $keyName = $this->_driver->quoteIdentifier($keyName);
            }
            return $prefix . sprintf(
                ' FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s',
                implode(', ', $columns),
                $keyName,
                $this->_convertConstraintColumns($data['references'][1]),
                $this->_foreignOnClause($data['delete'])
            );
        }
        return $prefix . ' (' . implode(', ', $columns) . ')';
    }

    /**
     * {@inheritDoc}
     * Override to only use quoteIdentifier if autoQuoting is enabled
     */
    protected function _convertConstraintColumns($references)
    {
        if ($this->_driver->autoQuoting()) {
            if (is_string($references)) {
                return $this->_driver->quoteIdentifier($references);
            }

            return implode(', ', array_map(
                [$this->_driver, 'quoteIdentifier'],
                $references
            ));
        } else {
            if (is_string($references)) {
                return $references;
            }
            return implode(', ', $references);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function constraintSql(TableSchema $table, $name)
    {
        $data = $table->constraint($name);
        if ($this->_driver->autoQuoting()) {
            $out = 'CONSTRAINT ' . $this->_driver->quoteIdentifier($name);
        } else {
            $out = 'CONSTRAINT ' . $name;
        }
        if ($data['type'] === TableSchema::CONSTRAINT_PRIMARY) {
            $out = 'PRIMARY KEY';
        }
        if ($data['type'] === TableSchema::CONSTRAINT_UNIQUE) {
            $out .= ' UNIQUE';
        }
        return $this->_keySql($out, $data);
    }

    /**
     * {@inheritDoc}
     */
    public function createTableSql(TableSchema $table, $columns, $constraints, $indexes)
    {
        $content = array_merge($columns, $constraints);
        $content = implode(",\n", array_filter($content));
        $tableName = $table->name();
        if ($this->_driver->autoQuoting()) {
            $tableName = $this->_driver->quoteIdentifier($tableName);
        }
        $out = [sprintf("CREATE TABLE %s (\n%s\n)", $tableName, $content)];
        foreach ($indexes as $index) {
            $out[] = $index;
        }
        foreach ($table->columns() as $column) {
            $columnData = $table->column($column);
            if ($this->_driver->autoQuoting()) {
                $column = $this->_driver->quoteIdentifier($column);
            }
            if (isset($columnData['comment'])) {
                $out[] = sprintf(
                    'COMMENT ON COLUMN %s.%s IS %s',
                    $tableName,
                    $column,
                    $this->_driver->schemaValue($columnData['comment'])
                );
            }
        }

        return $out;
    }

    /**
     * {@inheritDoc}
     */
    public function indexSql(TableSchema $table, $name)
    {
        throw new Oci8Exception("indexSql has not been implemented");
    }

    /**
     * {@inheritDoc}
     */
    public function listTablesSql($config)
    {
        if ($this->_driver->isAutoQuotingEnabled()) {
            $column = 'table_name';
        } else {
            $column = 'LOWER(table_name)';
        }
        if (empty($config['schema'])) {
            return ["SELECT {$column} FROM sys.user_tables", []];
        }
        return ["SELECT {$column} FROM sys.all_tables WHERE owner = :bindOwner", [':bindOwner' => strtoupper($config['schema'])]];
    }

    /**
     * {@inheritDoc}
     */
    public function truncateTableSql(TableSchema $table)
    {
        $tableName = $table->name();
        if ($this->_driver->autoQuoting()) {
            $tableName = $this->_driver->quoteIdentifier($tableName);
        }
        return [sprintf("TRUNCATE TABLE %s", $tableName)];
    }

    /**
     * {@inheritDoc}
     */
    public function addConstraintSql(TableSchema $table)
    {
        $sqlPattern = 'ALTER TABLE %s ADD %s;';
        $sql = [];

        foreach ($table->constraints() as $name) {
            $constraint = $table->constraint($name);
            if ($constraint['type'] === TableSchema::CONSTRAINT_FOREIGN) {
                if ($this->_driver->autoQuoting()) {
                    $tableName = $this->_driver->quoteIdentifier($table->name());
                } else {
                    $tableName = $table->name();
                }
                $sql[] = sprintf($sqlPattern, $tableName, $this->constraintSql($table, $name));
            }
        }

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function dropConstraintSql(TableSchema $table)
    {
        $sqlPattern = 'ALTER TABLE %s DROP CONSTRAINT %s;';
        $sql = [];

        foreach ($table->constraints() as $name) {
            $constraint = $table->constraint($name);
            if ($constraint['type'] === TableSchema::CONSTRAINT_FOREIGN) {
                if ($this->_driver->autoQuoting()) {
                    $tableName = $this->_driver->quoteIdentifier($table->name());
                    $constraintName = $this->_driver->quoteIdentifier($name);
                } else {
                    $tableName = $table->name();
                    $constraintName = $name;
                }
                $sql[] = sprintf($sqlPattern, $tableName, $constraintName);
            }
        }

        return $sql;
    }
}
