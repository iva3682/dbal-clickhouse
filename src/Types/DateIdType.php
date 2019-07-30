<?php
declare(strict_types = 1);

namespace FOD\DBALClickHouse\Types;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\DateType;

/**
 * DateTime type for UoW id, {@see DateTimeToString}.
 */
class DateIdType extends DateType
{
    /**
     * @inheritdoc
     */
    public function getName()
    {
        return 'date_id';
    }

    /**
     * @inheritdoc
     */
    public function convertToPHPValue($value, AbstractPlatform $platform)
    {
        return new DateTimeToString($value);
    }
}
