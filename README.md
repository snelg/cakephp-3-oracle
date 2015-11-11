# Oracle Datasource for CakePHP 3.x

This is an alpha version of an Oracle Datasource for CakePHP 3.0.
Currently the only functionality is basic data reading; Inserting,
deleting, and updating have not yet been tested.

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
            'schema' => 'SCHEMA_NAME',
        ],
    ]
```
