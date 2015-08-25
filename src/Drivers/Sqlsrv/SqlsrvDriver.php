<?php

/**
 * This file is part of the Nextras\Dbal library.
 * @license    MIT
 * @link       https://github.com/nextras/dbal
 */

namespace Nextras\Dbal\Drivers\Sqlsrv;

use DateTimeZone;
use Nextras\Dbal\Connection;
use Nextras\Dbal\ConnectionException;
use Nextras\Dbal\DriverException;
use Nextras\Dbal\Drivers\IDriver;
use Nextras\Dbal\InvalidArgumentException;
use Nextras\Dbal\NotImplementedException;
use Nextras\Dbal\NotSupportedException;
use Nextras\Dbal\Platforms\MsSqlPlatform;
use Nextras\Dbal\Result\Result;


class SqlsrvDriver implements IDriver
{
	/** @var resource */
	private $resource;

	/** @var DateTimeZone */
	private $simpleStorageTz;

	/** @var DateTimeZone */
	private $connectionTz;

	/** @var string */
	private $connectionString;

	/** @var array */
	private $connectionOptions;

	/** @var int */
	private $affectedRows;


	public function __destruct()
	{
		$this->disconnect();
	}


	public function connect(array $params)
	{
		/**
		 * @see https://msdn.microsoft.com/en-us/library/ff628167.aspx
		 */
		$knownConnectionOptions = [
			'App', 'ApplicationIntent', 'AttachDbFileName', 'CharacterSet',
			'ConnectionPooling', 'Encrypt', 'Falover_Partner', 'LoginTimeout',
			'MultipleActiveResultSet', 'MultiSubnetFailover', 'QuotedId',
			'ReturnDatesAsStrings', 'Scrollable', 'Server', 'TraceFile', 'TraceOn',
			'TransactionIsolation', 'TrustServerCertificate', 'WSID'
		];

		$this->connectionString = isset($params['port']) ? $params['host'] . ',' . $params['port'] : $params['host'];

		$this->connectionOptions = [];
		foreach ($params as $key => $value) {
			if ($key === 'user')
				$this->connectionOptions['UID'] = $value;
			elseif ($key === 'password')
				$this->connectionOptions['PWD'] = $value;
			elseif ($key === 'dbname')
				$this->connectionOptions['Database'] = $value;
			elseif ($key === 'simpleStorageTz')
				$this->simpleStorageTz = new DateTimeZone($value);
			elseif ($key === 'connectionTz')
				$this->connectionTz = new DateTimeZone($value);
			elseif (in_array($key, $knownConnectionOptions))
				$this->connectionOptions[$key] = $value;
			elseif (!in_array($key, ['driver', 'host', 'port', 'database', 'username', 'sqlMode']))
				throw new InvalidArgumentException("Unknown connection option '$key'");
		}

		if (isset($connectionInfo['ReturnDatesAsStrings']))
			throw new NotSupportedException("SqlsrvDriver does not allow to modify 'ReturnDatesAsStrings' parameter.");
		else
			$this->connectionOptions['ReturnDatesAsStrings'] = true;

		if (!$this->reconnect())
			$this->throwErrors();
	}


	/**
	 * @return bool
	 */
	private function reconnect()
	{
		$this->disconnect();

		$result = sqlsrv_connect($this->connectionString, $this->connectionOptions);

		if ($result) {
			$this->resource = $result;
			return true;
		} else {
			return false;
		}
	}


	public function disconnect()
	{
		if ($this->isConnected()) {
			if (!sqlsrv_close($this->resource))
				$this->throwErrors();
			else
				$this->resource = null;
		}
	}


	public function isConnected()
	{
		return $this->resource !== null;
	}


	public function getResourceHandle()
	{
		return $this->resource;
	}


	public function query($query)
	{
		/**
		 * @see https://msdn.microsoft.com/en-us/library/ee376927(SQL.90).aspx
		 */
		$statement = sqlsrv_prepare($this->resource, $query, [], ['Scrollable' => SQLSRV_CURSOR_STATIC]);

		if (!$statement)
			$this->throwErrors();

		$time = microtime(TRUE);
		$executed = sqlsrv_execute($statement);
		$time = microtime(TRUE) - $time;

		if (!$executed)
			$this->throwErrors();

		$this->setLastAffectedRows();

		return new Result(new SqlsrvResultAdapter($statement), $this, $time);
	}


	private function setLastAffectedRows()
	{
		if (!$result = sqlsrv_query($this->resource, 'SELECT @@ROWCOUNT'))
			$this->throwErrors();

		if ($result = sqlsrv_fetch_array($result, SQLSRV_FETCH_NUMERIC))
			$this->affectedRows = $result[0];
		else
			$this->throwErrors();
	}


	public function getLastInsertedId($sequenceName = NULL)
	{
		return $this->query('SELECT @@IDENTITY')->fetchField();
	}


	public function getAffectedRows()
	{
		return $this->affectedRows;
	}


	public function createPlatform(Connection $connection)
	{
		return new MsSqlPlatform($connection);
	}


	public function getServerVersion()
	{
		return sqlsrv_server_info($this->resource)['SQLServerVersion'];
	}


	public function ping()
	{
		return sqlsrv_begin_transaction($this->resource) || $this->reconnect();
	}


	public function beginTransaction()
	{
		if (!sqlsrv_begin_transaction($this->resource))
			$this->throwErrors();
	}


	public function commitTransaction()
	{
		if (!sqlsrv_commit($this->resource))
			$this->throwErrors();
	}


	public function rollbackTransaction()
	{
		if (!sqlsrv_rollback($this->resource))
			$this->throwErrors();
	}


	public function convertToPhp($value, $nativeType)
	{
		if ($nativeType === SqlsrvResultAdapter::SQLTYPE_BIGINT) {
			return is_float($tmp = $value * 1) ? $value : $tmp;

		} elseif (
			$nativeType === SqlsrvResultAdapter::SQLTYPE_DECIMAL_MONEY_SMALLMONEY ||
			$nativeType === SqlsrvResultAdapter::SQLTYPE_NUMERIC
		) {
			$float = (float)$value;
			$string = (string)$float;
			return $value === $string ? $float : $value;

		} elseif (
			$nativeType === SqlsrvResultAdapter::SQLTYPE_DATE ||
			$nativeType === SqlsrvResultAdapter::SQLTYPE_DATETIME_DATETIME2_SMALLDATETIME ||
			$nativeType === SqlsrvResultAdapter::SQLTYPE_TIME
		) {
			return $value . ' ' . $this->simpleStorageTz->getName();

		} else {
			throw new NotSupportedException("SqlsrvDriver does not support '{$nativeType}' type conversion.");

		}
	}


	public function convertToSql($value, $type)
	{
		switch ($type) {

			case self::TYPE_STRING:
				return "'" . str_replace("'", "''", $value) . "'";

			case self::TYPE_BOOL:
				return $value ? '1' : '0';

			case self::TYPE_IDENTIFIER:
				return str_replace('[*]', '*', '[' . str_replace([']', '.'], [']]', '].['], $value) . ']');

			case self::TYPE_BLOB:
				return '0x' . $value;

			case self::TYPE_DATETIME_SIMPLE:
				if ($value->getTimezone()->getName() !== $this->simpleStorageTz->getName()) {
					$value = clone $value;
					$value->setTimeZone($this->simpleStorageTz);
				}
				return "'" . $value->format('Y-m-d H:i:s') . "'";

			case self::TYPE_DATETIME:
				if ($value->getTimezone()->getname() !== $this->connectionTz->getName()) {
					$value = clone $value;
					$value->setTimeZone($this->connectionTz);
				}

				$tz = ($s = $value->getOffset()) >= 0 ? '+' . gmdate('H:i', $s) : '-' . gmdate('H:i', abs($s));
				return "'" . $value->format('Y-m-d H:i:s') . " " . $tz . "'";

			default:
				throw new InvalidArgumentException();

		}
	}


	public function modifyLimitQuery($query, $limit, $offset)
	{
		throw new NotImplementedException();
	}


	private function throwErrors()
	{
		$errors = sqlsrv_errors(SQLSRV_ERR_ERRORS);
		$errors = array_unique($errors, SORT_REGULAR);
		$errors = array_reverse($errors);

		$exception = NULL;
		foreach ($errors as $error) {
			$exception = $this->createException(
				$error['message'],
				$error['code'],
				$error['SQLSTATE'],
				NULL,
				$exception
			);
		}

		throw $exception;
	}


	protected function createException($error, $errorNo, $sqlState, $query = NULL, $previousException = NULL)
	{
		if (in_array($sqlState, ['HYT00', '08001', '28000', '42000']))
			return new ConnectionException($error, $errorNo, $sqlState, $previousException);
		else
			return new DriverException($error, $errorNo, $sqlState, $previousException);
	}

}