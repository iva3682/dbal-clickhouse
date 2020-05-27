<?php
declare(strict_types = 1);

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

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Connection as BaseConnection;

/**
 * ClickHouse Connection
 */
class Connection extends BaseConnection
{
    /**
     * {@inheritDoc}
     */
    public function executeUpdate($query, array $params = [], array $types = []): int
    {
        $query = \str_replace(' SET ', ' UPDATE ', \str_replace('UPDATE ', ' ', $query));
        foreach ($types as &$type) {
            $type = $type === 'float' ? 'integer' : $type;
        } unset($type);

        if (\stripos(\trim($query), 'DROP ') === 0) {
            return parent::executeUpdate($query, $params, $types);
        }

        if (\stripos(\trim($query), 'DELETE FROM') === 0) {
            return parent::executeUpdate(\preg_replace('/DELETE FROM (.*?) /', 'ALTER TABLE ${1} DELETE ', $query), $params, $types);
        }

        if (\stripos($query, 'CREATE ') === false && \stripos($query, 'INSERT INTO') === false) {
            return parent::executeUpdate('ALTER TABLE ' . $query, $params, $types);
        }

        return parent::executeUpdate($query, $params, $types);
    }

    /**
     * {@inheritdoc}
     */
    public function delete($tableExpression, array $identifier, array $types = []): int
    {
        if (empty($identifier)) {
            throw InvalidArgumentException::fromEmptyCriteria();
        }

        [$columns, $values, $conditions] = $this->gatherConditions($identifier);

        return $this->executeUpdate(
            $tableExpression . ' DELETE WHERE ' . \implode(' AND ', $conditions),
            $values,
            \is_string(\key($types)) ? $this->extractTypeValues($columns, $types) : $types
        );
    }

    /**
     * @inheritdoc
     */
    public function update($tableExpression, array $data, array $identifier, array $types = []): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @inheritdoc
     */
    public function setTransactionIsolation($level): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function getTransactionIsolation(): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function getTransactionNestingLevel(): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function transactional(\Closure $func): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function getNestTransactionsWithSavepoints(): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function beginTransaction(): void
    {
    }

    /**
     * @inheritdoc
     */
    public function commit(): void
    {
    }

    /**
     * @inheritdoc
     */
    public function rollBack(): void
    {
    }

    /**
     * @inheritdoc
     */
    public function createSavepoint($savepoint): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function releaseSavepoint($savepoint): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function rollbackSavepoint($savepoint): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function setRollbackOnly(): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritdoc
     */
    public function isRollbackOnly(): void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * Extract ordered type list from an ordered column list and type map.
     *
     * @param array $columnList
     * @param array $types
     *
     * @return array
     */
    protected function extractTypeValues(array $columnList, array $types)
    {
        $typeValues = [];

        foreach ($columnList as $columnName) {
            $typeValues[] = $types[$columnName] ?? ParameterType::STRING;
        }

        return $typeValues;
    }

    /**
     * Collect conditions.
     *
     * @param array $identifiers
     *
     * @return array
     */
    protected function gatherConditions(array $identifiers): array
    {
        $columns = [];
        $values = [];
        $conditions = [];

        foreach ($identifiers as $columnName => $value) {
            if (null === $value) {
                $conditions[] = $this->getDatabasePlatform()->getIsNullExpression($columnName);
                continue;
            }

            $columns[] = $columnName;
            $values[] = $value;
            $conditions[] = $columnName . ' = ?';
        }

        return [$columns, $values, $conditions];
    }
}
