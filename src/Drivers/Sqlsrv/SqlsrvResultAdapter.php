<?php

/**
 * This file is part of the Nextras\Dbal library.
 * @license    MIT
 * @link       https://github.com/nextras/dbal
 */

namespace Nextras\Dbal\Drivers\Sqlsrv;

use Nextras\Dbal\Drivers\IResultAdapter;
use Nextras\Dbal\InvalidStateException;


class SqlsrvResultAdapter implements IResultAdapter
{
	const TYPE_DRIVER_SPECIFIC_AND_DATETIME = 33;

	/** @link https://msdn.microsoft.com/en-us/library/cc296197.aspx */
	const
		SQLTYPE_TIME = -154,
		SQLTYPE_DATE = 91,
		SQLTYPE_DATETIME_DATETIME2_SMALLDATETIME = 93,
		SQLTYPE_DATETIMEOFFSET = -155,

		SQLTYPE_NUMERIC = 2,
		SQLTYPE_DECIMAL_MONEY_SMALLMONEY = 3,

		SQLTYPE_BIGINT = SQLSRV_SQLTYPE_BIGINT,
		SQLTYPE_BIT = SQLSRV_SQLTYPE_BIT;

	/** @var array */
	protected static $types = [
		self::SQLTYPE_BIGINT => self::TYPE_INT,
		self::SQLTYPE_BIT => self::TYPE_BOOL,

		self::SQLTYPE_NUMERIC => self::TYPE_DRIVER_SPECIFIC,
		self::SQLTYPE_DECIMAL_MONEY_SMALLMONEY => self::TYPE_DRIVER_SPECIFIC,

		self::SQLTYPE_TIME => self::TYPE_DRIVER_SPECIFIC,
		self::SQLTYPE_DATE => self::TYPE_DRIVER_SPECIFIC_AND_DATETIME,
		self::SQLTYPE_DATETIME_DATETIME2_SMALLDATETIME => self::TYPE_DRIVER_SPECIFIC_AND_DATETIME,
		self::SQLTYPE_DATETIMEOFFSET => self::TYPE_DATETIME
	];

	private $result;


	public function __construct($result)
	{
		$this->result = $result;

		if (PHP_INT_SIZE < 8)
			self::$types[SQLSRV_SQLTYPE_BIGINT] = self::TYPE_DRIVER_SPECIFIC;
	}


	public function seek($index)
	{
		if ($index !== 0 && !sqlsrv_fetch($this->result, SQLSRV_SCROLL_ABSOLUTE, $index - 1))
			throw new InvalidStateException("Unable to seek in row set to {$index} index.");
	}


	public function fetch()
	{
		return sqlsrv_fetch_array($this->result, SQLSRV_FETCH_ASSOC);
	}


	public function getTypes()
	{
		$types = [];
		$fields = sqlsrv_field_metadata($this->result);
		foreach ($fields as $field) {
			$nativeType = $field['Type'];
			$types[$field['Name']] = [
				0 => isset(self::$types[$nativeType]) ? self::$types[$nativeType] : self::TYPE_AS_IS,
				1 => $nativeType
			];
		}

		return $types;
	}

}