<?php
namespace Cake\Oracle;

use Cake\Database\QueryCompiler;

class OracleCompiler extends QueryCompiler
{
    public function __construct()
    {
        //"offset" and "limit" do not work in pre-12.1 Oracle, so skipping them completely
        $this->_selectParts = [
            'select', 'from', 'join', 'where', 'group', 'having', 'order', 'union', 'epilog'];
    }
}
