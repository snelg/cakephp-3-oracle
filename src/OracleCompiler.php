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
namespace Cake\Oracle;

use Cake\Database\ExpressionInterface;
use Cake\Database\QueryCompiler;

class OracleCompiler extends QueryCompiler
{
    /**
     * {@inheritDoc}
     */
    protected $_templates = [
        'delete' => 'DELETE',
        'update' => 'UPDATE %s',
        'where' => ' WHERE %s',
        'group' => ' GROUP BY %s ',
        'having' => ' HAVING %s ',
        'order' => ' %s'
    ];

    /**
     * The list of query clauses to traverse for generating a SELECT statement
     * Since Oracle (before version 12c) does not have built-in offset and limit,
     * we need to add some funky wrapper sql. So we traverse "offset" and "limit"
     * *before* "select", and also tweak the "epilog" as necessary
     * Please let me know if there's a better way to add "wrapper" sql
     * than this :)
     *
     * @var array
     */
    protected $_selectParts = [
        'offset', 'limit',
        'select', 'from', 'join', 'where', 'group', 'having', 'order', 'union',
        'epilog'
    ];

    /**
     * Generates the OFFSET part of a SQL query.
     * Due to the way Oracle ROWNUM works, if you want an offset *without*
     * a limit, you still need to add a similar subquery as you use with
     * a limit.
     *
     * @param int $offset the offset clause
     * @param \Cake\Database\Query $query The query that is being compiled
     * @return string
     */
    protected function _buildOffsetPart($offset, $query)
    {
        if (intval($offset) < 1) {
            return '';
        }

        $origEpilog = $query->clause('epilog');
        if (is_array($origEpilog) && array_key_exists('snelgOracleOrigEpilog', $origEpilog)) {
            $origEpilog = $origEpilog['snelgOracleOrigEpilog'];
        }

        $limit = intval($query->clause('limit'));
        if ($limit < 1) {
            $offsetEndWrap = ") a) WHERE snelg_oracle_sub_rnum > $offset";
        } else {
            $offsetEndWrap = ") WHERE snelg_oracle_sub_rnum > $offset";
        }

        /*
         * It would be more efficient to use bind vars here, e.g.
         *
         * $query->bind(':SNELG_FETCH_OFFSET', $offset);
         *
         * EXCEPT that additional calls to $query->count() (e.g. in Paginator
         * component) reset the Query "limit" and "offset" to null.
         * In that case, the re-run Query would try to bind to the a
         * non-existent bind var
         */
        $query->epilog([
            'snelgOracleOrigEpilog' => $origEpilog,
            'snelgOracleOffsetEndWrap' => $offsetEndWrap
        ]);

        if ($limit < 1) {
            return 'SELECT * FROM (SELECT /*+ FIRST_ROWS(n) */ a.*, ROWNUM snelg_oracle_sub_rnum FROM (';
        } else {
            return 'SELECT * FROM (';
        }
    }

    /**
     * Generates the LIMIT part of a SQL query
     *
     * @param int $limit the limit clause
     * @param \Cake\Database\Query $query The query that is being compiled
     * @return string
     */
    protected function _buildLimitPart($limit, $query)
    {
        if (intval($limit) < 1) {
            return '';
        }

        $endRow = intval($query->clause('offset')) + $limit;
        $origEpilog = $query->clause('epilog');
        $offsetEndWrap = '';
        if (is_array($origEpilog) && array_key_exists('snelgOracleOrigEpilog', $origEpilog)) {
            $offsetEndWrap = empty($origEpilog['snelgOracleOffsetEndWrap']) ? '' : $origEpilog['snelgOracleOffsetEndWrap'];
            $origEpilog = $origEpilog['snelgOracleOrigEpilog'];
        }

        //See note in _buildOffsetPart about ->bind being potentially
        //more efficient here
        $query->epilog([
            'snelgOracleOrigEpilog' => $origEpilog,
            'snelgOracleLimitEndWrap' => ") a WHERE ROWNUM <= $endRow",
            'snelgOracleOffsetEndWrap' => $offsetEndWrap
        ]);

        return 'SELECT /*+ FIRST_ROWS(n) */ a.*, ROWNUM snelg_oracle_sub_rnum FROM (';
    }

    /**
     * Generates the EPILOG part of a SQL query, including special handling
     * if we added offset and/or limit wrappers earlier
     *
     * @param mixed $epilog the epilog clause
     * @param \Cake\Database\Query $query The query that is being compiled
     * @param \Cake\Database\ValueBinder $generator The placeholder and value binder object
     * @return string
     */
    protected function _buildEpilogPart($epilog, $query, $generator)
    {
        if (!is_array($epilog) || !array_key_exists('snelgOracleOrigEpilog', $epilog)) {
            $origEpilog = $epilog;
        } else {
            //We wrapped the original epilog, which might have been an
            //ExpressionInterface instead of a simple string
            $origEpilog = $epilog['snelgOracleOrigEpilog'];
        }

        //Duplicate original _sqlCompiler functionality...
        if ($origEpilog instanceof ExpressionInterface) {
            $origEpilog = [$origEpilog->sql($generator)];
        }
        $origEpilog = $this->_stringifyExpressions((array)$origEpilog, $generator);
        $epilogSql = sprintf(' %s', implode(', ', $origEpilog));

        //...and then add our own wrappers.
        /*
         * We need to double-check that "limit" and/or "offset"
         * are actually set because calls to $query->count() (e.g. in Paginator
         * component) reset the Query "limit" and "offset" to null
         */
        if (is_array($epilog)) {
            if (!empty($epilog['snelgOracleLimitEndWrap'])
                    && intval($query->clause('limit') > 0)) {
                $epilogSql .= $epilog['snelgOracleLimitEndWrap'];
            }
            if (!empty($epilog['snelgOracleOffsetEndWrap'])
                    && intval($query->clause('offset') > 0)) {
                $epilogSql .= $epilog['snelgOracleOffsetEndWrap'];
            }
        }

        return $epilogSql;
    }
}
