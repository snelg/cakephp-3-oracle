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
namespace Cake\Oracle\Dialect;

use Cake\Database\SqlDialectTrait;
use Cake\Oracle\OracleCompiler;

trait OracleDialectTrait
{
    use SqlDialectTrait {
        quoteIdentifier as origQuoteIdentifier;
    }

    protected $_schemaDialect;

    protected $_startQuote = '"';
    protected $_endQuote = '"';

    public $autoShortenedIdentifiers = [];

    /**
     * {@inheritDoc}
     *
     * @return \Cake\Oracle\OracleCompiler
     */
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

        $newQuery = $query->getConnection()->newQuery();
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

            $q = $newQuery->getConnection()->newQuery();
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

    /**
     * VERY HACKY: To avoid Oracle's "No identifiers > 30 characters"
     * restriction, at this very low level we'll auto-replace Cake automagic
     * aliases like 'SomeLongTableName__some_really_long_field_name' with
     * 'XXAUTO_SHORTENED_ID[n]' where [n] is a simple incrementing integer.
     * Then in OracleStatement's "fetch" function, we'll undo these
     * auto-replacements
     *
     * {@inheritDoc}
     */
    public function quoteIdentifier($identifier)
    {
        if (preg_match('/^[\w-]+$/', $identifier) && strlen($identifier) > 30) {
            $key = array_search($identifier, $this->autoShortenedIdentifiers);
            if ($key === false) {
                $key = 'XXAUTO_SHORTENED_ID' . (count($this->autoShortenedIdentifiers) + 1);
                $this->autoShortenedIdentifiers[$key] = $identifier;
            }
            $identifier = $key;
        }
        return $this->origQuoteIdentifier($identifier);
    }
}
