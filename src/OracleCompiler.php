<?php
namespace Cake\Oracle;

use Cake\Database\QueryCompiler;

class OracleCompiler extends QueryCompiler
{
    /**
     * {@inheritDoc}
     */
    protected $_selectParts = [
        'select', 'from', 'join', 'where', 'group', 'having', 'order', 'union',
        'epilog'
    ];
}
