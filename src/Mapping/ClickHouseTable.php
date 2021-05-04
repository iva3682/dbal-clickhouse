<?php
declare(strict_types = 1);

namespace FOD\DBALClickHouse\Mapping;

/**
 * @Annotation
 * @Target("CLASS")
 */
class ClickHouseTable
{
	public $engine;

	public $indexGranularity;

	public $viewQuery;
}
