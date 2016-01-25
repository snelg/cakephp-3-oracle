# Oracle Datasource for CakePHP 3.x

This is an alpha version of an Oracle Datasource for CakePHP 3.0.
Currently the only functionality is basic data reading; Inserting,
deleting, and updating have only been tested minimally.

## Installing via composer

Install into your project using [composer](http://getcomposer.org).
For existing applications you can add the
following to your composer.json file:

    "require": {
        "snelg/cakephp-3-oracle": "~1.0"
    }

And run `php composer.phar update`

## Defining a connection

Sample connection info:

```php
// in config/app.php
    'Datasources' => [
        // other datasources
        'my_oracle_db' => [
            'className' => 'Cake\Database\Connection',
            'driver' => 'Cake\Oracle\Driver\Oracle',
            /* 'host' => '', //Usually unused for Oracle connections */
            'username' => 'you know what goes here',
            'password' => 'and here',
            'database' => 'TNS entry name or full conn string, e.g. (DESCRIPTION=(ADDRESS_LIST=( [...] )))',
            'schema' => 'SCHEMA_NAME', //The schema that owns the tables, not necessarily your login schema
        ],
    ]
```

If your data tables are owned by a different schema than your login user, then make sure you put the table-owning schema name in the "schema" field instead of your login schema.

If you want to access data from multiple schemas, then you do *not* need multiple datasources. Instead, you can specify the schema in a Table's "initialize" function:

```php
class UsersTable extends Table
{
    public function initialize(array $config)
    {
        $this->table('some_other_schema.users');
    }
}
```