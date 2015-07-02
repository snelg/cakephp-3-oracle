<?php
namespace Cake\Oracle\Dialect;

use Cake\Database\SqlDialectTrait;

trait OracleDialectTrait
{
    use SqlDialectTrait;

    protected $_schemaDialect;

    protected $_startQuote = '"';
    protected $_endQuote = '"';

}