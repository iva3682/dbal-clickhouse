<?php
declare(strict_types = 1);

namespace FOD\DBALClickHouse\Types;

/**
 * DateTime object that supports __toString(), because UoW reqiure it in identifier.
 */
class DateTimeToString extends \DateTime
{
    /**
     * @inheritdoc
     */
    public function __toString()
    {
        return $this->format('U');
    }
}