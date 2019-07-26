<?php
declare(strict_types = 1);

namespace FOD\DBALClickHouse\Mapping;

use Doctrine\ORM\Mapping\Annotation;

/**
 * @Annotation
 * @Target("PROPERTY")
 */
final class VersionColumn implements Annotation
{

}