<?php
declare(strict_types = 1);

namespace FOD\DBALClickHouse\Types;

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\FloatType as BaseFloatType;

/**
 * ClickHouse float type.
 */
class FloatType extends BaseFloatType
{
    /**
     * @inheritdoc
     */
    public function getBindingType()
    {
        return ParameterType::INTEGER;
    }
}