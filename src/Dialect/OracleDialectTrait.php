<?php
namespace Cake\Oracle\Dialect;

use Cake\Database\SqlDialectTrait;
use Cake\Oracle\OracleCompiler;
use yajra\Pdo\Oci8\Exceptions\Oci8Exception;

trait OracleDialectTrait
{
    use SqlDialectTrait;

    protected $_schemaDialect;

    protected $_startQuote = '"';
    protected $_endQuote = '"';

    public function newCompiler()
    {
        return new OracleCompiler;
    }

    /**
     * Transforms an insert query that is meant to insert multiple rows at a time,
     * otherwise it leaves the query untouched.
     *
     * The way Oracle works with multi insert is by having multiple
     * "SELECT FROM DUAL" select statements joined with UNION.
     *
     * @param \Cake\Database\Query $query The query to translate
     * @return \Cake\Database\Query
     */
    protected function _insertQueryTranslator($query)
    {
        $v = $query->clause('values');
        if (count($v->values()) === 1 || $v->query()) {
            return $query;
        }

        $newQuery = $query->connection()->newQuery();
        $cols = $v->columns();
        $placeholder = 0;
        $replaceQuery = false;

        foreach ($v->values() as $k => $val) {
            $fillLength = count($cols) - count($val);
            if ($fillLength > 0) {
                $val = array_merge($val, array_fill(0, $fillLength, null));
            }

            foreach ($val as $col => $attr) {
                if (!($attr instanceof ExpressionInterface)) {
                    $val[$col] = sprintf(':c%d', $placeholder);
                    $placeholder++;
                }
            }

            $select = array_combine($cols, $val);
            if ($k === 0) {
                $replaceQuery = true;
                $newQuery->select($select)->from('DUAL');
                continue;
            }

            $q = $newQuery->connection()->newQuery();
            $newQuery->unionAll($q->select($select)->from('DUAL'));
        }

        if ($replaceQuery) {
            $v->query($newQuery);
        }

        return $query;
    }

    /**
     * Returns a SQL snippet for releasing a previously created save point
     *
     * @param string $name save point name
     * @return string
     */
    public function releaseSavePointSQL($name)
    {
        //Oracle doesn't have "release savepoint" functionality
        return '';
    }

}