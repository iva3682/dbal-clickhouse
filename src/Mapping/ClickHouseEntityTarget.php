<?php
declare(strict_types = 1);

namespace FOD\DBALClickHouse\Mapping;

/**
 * @Annotation
 * @Target("CLASS")
 */
class ClickHouseEntityTarget
{
	public $entityClass;
}
