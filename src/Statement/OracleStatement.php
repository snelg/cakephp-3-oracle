<?php
/*
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
 */
namespace Cake\Oracle\Statement;

use Cake\Database\Statement\BufferedStatement;
use Cake\Database\Statement\BufferResultsTrait;
use Cake\Database\Statement\StatementDecorator;

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

    /**
     * {@inheritDoc}
     */
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

    /**
     * VERY HACKY: Override fetch to UN-auto-shorten identifiers,
     * which is done in OracleDialectTrait "quoteIdentifier"
     *
     * {@inheritDoc}
     */
    public function fetch($type = 'num')
    {
        $row = parent::fetch($type);
        if ($type == 'assoc' && is_array($row) && !empty($this->_driver->autoShortenedIdentifiers)) {
            //Need to preserve order of row results
            $translatedRow = [];
            foreach ($row as $key => $val) {
                if (array_key_exists($key, $this->_driver->autoShortenedIdentifiers)) {
                    $translatedRow[$this->_driver->autoShortenedIdentifiers[$key]] = $val;
                } else {
                    $translatedRow[$key] = $val;
                }
            }
            $row = $translatedRow;
        }
        return $row;
    }
}
