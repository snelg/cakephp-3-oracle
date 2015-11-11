<?php
namespace Cake\Oracle\Schema;

use Cake\Database\Schema\Table;
use Cake\Database\Schema\BaseSchema;
use yajra\Pdo\Oci8\Exceptions\Oci8Exception;

/**
 * Schema generation/reflection features for Oracle
 */
class OracleSchema extends BaseSchema
{
    public function describeColumnSql($tableName, $config)
    {
        list($table, $schema) = $this->_tableSplit($tableName, $config);
        if (empty($schema)) {
            return [
                'SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT '
                . 'FROM user_tab_columns WHERE table_name = :bindTable', [
                    ':bindTable' => $table]];
        }
        return [
            'SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT '
            . 'FROM all_tab_columns WHERE table_name = :bindTable AND owner = :bindOwner', [
                ':bindTable' => $table,
                ':bindOwner' => $schema]];
    }

    public function describeIndexSql($tableName, $config)
    {
        list($table, $schema) = $this->_tableSplit($tableName, $config);
        if (empty($schema)) {
            return [
                'SELECT cc.table_name, cc.column_name, cc.constraint_name, c.constraint_type, i.index_name, i.uniqueness '
                . 'FROM user_cons_columns cc '
                . 'LEFT JOIN user_indexes i ON(i.index_name  = cc.constraint_name) '
                . 'LEFT JOIN user_constraints c ON(c.constraint_name = cc.constraint_name) '
                . 'WHERE cc.table_name = :bindTable', [
                    ':bindTable' => $table]];
        }
        return [
            'SELECT cc.table_name, cc.column_name, cc.constraint_name, c.constraint_type, i.index_name, i.uniqueness '
            . 'FROM all_cons_columns cc '
            . 'LEFT JOIN all_indexes i ON(i.index_name  = cc.constraint_name AND cc.owner = i.owner) '
            . 'LEFT JOIN all_constraints c ON(c.constraint_name = cc.constraint_name AND c.owner = cc.owner) '
            . 'WHERE cc.table_name = :bindTable '
            . 'AND cc.owner = :bindOwner', [
                ':bindTable' => $table,
                ':bindOwner' => $schema]];
    }

    public function describeForeignKeySql($tableName, $config)
    {
        list($table, $schema) = $this->_tableSplit($tableName, $config);
        if (empty($schema)) {
            return [
                'SELECT cc.column_name, cc.constraint_name, r.owner AS REFERENCED_OWNER, r.table_name AS REFERENCED_TABLE_NAME, r.column_name AS REFERENCED_COLUMN_NAME, c.delete_rule '
                . 'FROM user_cons_columns cc '
                . 'JOIN user_constraints c ON(c.constraint_name = cc.constraint_name) '
                . 'JOIN user_cons_columns r ON(r.constraint_name = c.r_constraint_name) '
                . "WHERE c.constraint_type = 'R' "
                . 'AND cc.table_name = :bindTable', [
                    ':bindTable' => $table]];
        }
        return [
            'SELECT cc.column_name, cc.constraint_name, r.owner AS REFERENCED_OWNER, r.table_name AS REFERENCED_TABLE_NAME, r.column_name AS REFERENCED_COLUMN_NAME, c.delete_rule '
            . 'FROM all_cons_columns cc '
            . 'JOIN all_constraints c ON(c.constraint_name = cc.constraint_name AND c.owner = cc.owner) '
            . 'JOIN all_cons_columns r ON(r.constraint_name = c.r_constraint_name AND r.owner = c.r_owner) '
            . "WHERE c.constraint_type = 'R' "
            . 'AND cc.table_name = :bindTable '
            . 'AND cc.owner = :bindOwner', [
                ':bindTable' => $table,
                ':bindOwner' => $schema]];
    }

    public function convertColumnDescription(Table $table, $row)
    {
        switch($row['DATA_TYPE']) {
            case 'DATE':
                $field = ['type' => 'datetime', 'length' => null];
                break;
            case 'TIMESTAMP':
            case 'TIMESTAMP(6)':
            case 'TIMESTAMP(9)':
                $field = ['type' => 'timestamp', 'length' => null];
                break;
            case 'NUMBER':
                if ($row['DATA_PRECISION'] == 1) {
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
                $field = ['type' => 'string', 'length' => $row['DATA_LENGTH']];
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

    public function convertIndexDescription(Table $table, $row)
    {
        $type = null;
        $columns = $length = [];

        $name = $row['CONSTRAINT_NAME'];
        switch($row['CONSTRAINT_TYPE']) {
            case 'P':
                $name = $type = Table::CONSTRAINT_PRIMARY;
                break;
            case 'U':
                $type = Table::CONSTRAINT_UNIQUE;
                break;
            default:
                return; //Not doing anything here with Oracle "Check" constraints or "Reference" constraints
        }

        $columns[] = strtolower($row['COLUMN_NAME']);

        $isIndex = (
            $type === Table::INDEX_INDEX ||
            $type === Table::INDEX_FULLTEXT
        );
        if ($isIndex) {
            $existing = $table->index($name);
        } else {
            $existing = $table->constraint($name);
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

    public function convertForeignKeyDescription(Table $table, $row)
    {
        $data = [
            'type' => Table::CONSTRAINT_FOREIGN,
            'columns' => [strtolower($row['COLUMN_NAME'])],
            'references' => ["{$row['REFERENCED_OWNER']}.{$row['REFERENCED_TABLE_NAME']}", strtolower($row['REFERENCED_COLUMN_NAME'])],
            'update' => Table::ACTION_SET_NULL,
            'delete' => $this->_convertOnClause($row['DELETE_RULE']),
        ];
        $name = $row['CONSTRAINT_NAME'];
        $table->addConstraint($name, $data);
    }

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

    public function columnSql(Table $table, $name)
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

        if ($data['type'] === Table::CONSTRAINT_FOREIGN) {
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
     * Convert foreign key constraints references to a valid
     * stringified list (override to only use quoteIdentifier if autoQuoting is
     * enabled)
     *
     * @param string|array $references The referenced columns of a foreign key constraint statement
     * @return string
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
    public function constraintSql(Table $table, $name)
    {
        $data = $table->constraint($name);
        if ($this->_driver->autoQuoting()) {
            $out = 'CONSTRAINT ' . $this->_driver->quoteIdentifier($name);
        } else {
            $out = 'CONSTRAINT ' . $name;
        }
        if ($data['type'] === Table::CONSTRAINT_PRIMARY) {
            $out = 'PRIMARY KEY';
        }
        if ($data['type'] === Table::CONSTRAINT_UNIQUE) {
            $out .= ' UNIQUE';
        }
        return $this->_keySql($out, $data);
    }

    public function createTableSql(Table $table, $columns, $constraints, $indexes)
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

    public function indexSql(Table $table, $name)
    {
        throw new Oci8Exception("indexSql has not been implemented");
    }

    public function listTablesSql($config)
    {
        if (empty($config['schema'])) {
            return ['SELECT table_name FROM sys.user_tables', []];
        }
        return ['SELECT table_name FROM sys.all_tables WHERE owner = :bindOwner', [':bindOwner' => strtoupper($config['schema'])]];
    }

    public function truncateTableSql(Table $table)
    {
        $tableName = $table->name();
        if ($this->_driver->autoQuoting()) {
            $tableName = $this->_driver->quoteIdentifier($tableName);
        }
        return [sprintf("TRUNCATE TABLE %s", $tableName)];
    }

    public function addConstraintSql(Table $table)
    {
        $sqlPattern = 'ALTER TABLE %s ADD %s;';
        $sql = [];

        foreach ($table->constraints() as $name) {
            $constraint = $table->constraint($name);
            if ($constraint['type'] === Table::CONSTRAINT_FOREIGN) {
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

    public function dropConstraintSql(Table $table)
    {
        $sqlPattern = 'ALTER TABLE %s DROP CONSTRAINT %s;';
        $sql = [];

        foreach ($table->constraints() as $name) {
            $constraint = $table->constraint($name);
            if ($constraint['type'] === Table::CONSTRAINT_FOREIGN) {
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
