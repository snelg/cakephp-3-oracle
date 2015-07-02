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

    public function describeIndexSql($tableName, $config) {
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

    public function describeForeignKeySql($tableName, $config) {
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
            case 'VARCHAR2':
                $field = ['type' => 'string', 'length' => $row['DATA_LENGTH']];
                break;
            default:
        }
        $field += [
            'null' => $row['NULLABLE'] === 'Y' ? true : false,
            'default' => $row['DATA_DEFAULT']];
        $table->addColumn(strtolower($row['COLUMN_NAME']), $field);
    }

    public function convertIndexDescription(Table $table, $row) {
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

    public function convertForeignKeyDescription(Table $table, $row) {
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

    protected function _tableSplit($tableName, $config) {
        $schema = null;
        $table = strtoupper($tableName);
        if (strpos($tableName, '.') !== false) {
            $tableSplit = explode('.', $tableName);
            $table = $tableSplit[1];
            $schema = $tableSplit[0];
        } elseif (!empty($config['schema'])) {
            $schema = strtoupper($config['schema']);
        }
        return [$table, $schema];
    }

    public function columnSql(Table $table, $name) {
        throw new Oci8Exception("columnSql has not been implemented");
    }

    public function constraintSql(Table $table, $name) {
        throw new Oci8Exception("constraintSql has not been implemented");
    }

    public function createTableSql(Table $table, $columns, $constraints, $indexes) {
        throw new Oci8Exception("createTableSql has not been implemented");
    }

    public function indexSql(Table $table, $name) {
        throw new Oci8Exception("indexSql has not been implemented");
    }

    public function listTablesSql($config) {
        throw new Oci8Exception("listTablesSql has not been implemented");
    }

    public function truncateTableSql(Table $table) {
        throw new Oci8Exception("truncateTableSql has not been implemented");
    }
}
