<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\View;
use Doctrine\DBAL\Types\Type;

/**
 * Schema manager for the ClickHouse DBMS.
 */
class ClickHouseSchemaManager extends AbstractSchemaManager
{
    /**
     * {@inheritdoc}
     */
    public function listTableIndexes($table): array
    {
        $tableView = $this->_getPortableViewDefinition(['name' => $table]);

        \preg_match(
            '/MergeTree\(([\w+, \(\)]+)(?= \(((?:[^()]|\((?2)\))+)\),)/mi',
            $tableView->getSql(),
            $matches
        );

        if (\is_array($matches) && \array_key_exists(2, $matches)) {
            $indexColumns = \array_filter(
                \array_map('trim', \explode(',', $matches[2])),
                static function (string $column) {
                    return \strpos($column, '(') === false;
                }
            );

            return [
                new Index(
                    \current(\array_reverse(\explode('.', $table))) . '__pk',
                    $indexColumns,
                    false,
                    true
                ),
            ];
        }

        if(preg_match('/ORDER\s+BY\s+[(]*([\w,\s]+)[)]*[\s+](?:SETTINGS|TTL|SAMPLE|PRIMARY|PARTITION)/isU', $tableView->getSql(), $match)) {
            $indexColumns = \array_filter(
                \array_map('trim', \explode(',', $match[1])),
                static function (string $column) {
                    return \strpos($column, '(') === false;
                }
            );

            return [
                new Index(
                    \current(\array_reverse(\explode('.', $table))) . '__pk',
                    $indexColumns,
                    false,
                    true
                ),
            ];
        }

        return [];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnDefinition($tableColumn): Column
    {
        $tableColumn = \array_change_key_case($tableColumn, \CASE_LOWER);

        $dbType  = $columnType = \trim($tableColumn['type']);
        $length  = null;
        $fixed   = false;
        $notnull = true;

        if (\preg_match('/(Nullable\((\w+)\))/i', $columnType, $matches)) {
            $columnType = \str_replace($matches[1], $matches[2], $columnType);
            $notnull    = false;
        }

        if (\strncasecmp($columnType, 'fixedstring', 11) === 0) {
            // get length from FixedString definition
            $length = \preg_replace('~.*\(([0-9]*)\).*~', '$1', $columnType);
            $dbType = 'fixedstring';
            $fixed  = true;
        }

        if (\strncasecmp($columnType, 'string', 6) === 0) {
            $length = 0;
        }

        $unsigned = false;
        if (\strncasecmp($columnType, 'uint', 4) === 0) {
            $unsigned = true;
        }

        if (!isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        $default = null;
        //TODO process not only DEFAULT type, but ALIAS and MATERIALIZED too
        if ($tableColumn['default_expression'] && \strtolower($tableColumn['default_type']) === 'default') {
            $default = $tableColumn['default_expression'];
        }

        $options = [
            'length' => $length,
            'notnull' => $notnull,
            'default' => $default,
            'primary' => false,
            'fixed' => $fixed,
            'unsigned' => $unsigned,
            'autoincrement' => false,
            'comment' => null,
        ];

        return new Column(
            $tableColumn['name'],
            Type::getType($this->_platform->getDoctrineTypeMapping($dbType)),
            $options
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableDatabaseDefinition($database)
    {
        return $database['name'];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableDefinition($table)
    {
        if(strncasecmp($table['name'], '.inner.', 7) === 0) {
            return '';
        }

        return $table['name'];
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableViewDefinition($view)
    {
        $statement = $this->_conn->fetchColumn('SHOW CREATE TABLE `' . $view['name'] . '`');

        return new View($view['name'], $statement);
    }
}
