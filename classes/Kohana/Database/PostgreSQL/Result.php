<?php defined('SYSPATH') OR die('No direct script access.');

/**
 * PostgreSQL database result.
 *
 * @package     PostgreSQL
 * @author      Chris Bandy
 * @copyright   (c) 2010 Chris Bandy
 * @license     http://www.opensource.org/licenses/isc-license.txt
 */
class Kohana_Database_PostgreSQL_Result extends Database_Result
{
	public function __construct($result, $sql, $as_object = FALSE, $params = NULL, $total_rows = NULL)
	{
		parent::__construct($result, $sql, $as_object, $params);

		if ($as_object === TRUE)
		{
			$this->_as_object = 'stdClass';
		}

		if ($total_rows !== NULL)
		{
			$this->_total_rows = $total_rows;
		}
		else
		{
			switch (pg_result_status($result))
			{
				case PGSQL_TUPLES_OK:
					$this->_total_rows = pg_num_rows($result);
				break;
				case PGSQL_COMMAND_OK:
					$this->_total_rows = pg_affected_rows($result);
				break;
				case PGSQL_BAD_RESPONSE:
				case PGSQL_NONFATAL_ERROR:
				case PGSQL_FATAL_ERROR:
					throw new Database_Exception(':error [ :query ]',
						array(':error' => pg_result_error($result), ':query' => $sql));
				case PGSQL_COPY_OUT:
				case PGSQL_COPY_IN:
					throw new Database_Exception('PostgreSQL COPY operations not supported [ :query ]',
						array(':query' => $sql));
				default:
					$this->_total_rows = 0;
			}
		}
	}

	public function __destruct()
	{
		if (is_resource($this->_result))
		{
			pg_free_result($this->_result);
		}
	}

	protected function fixType($value, $column)
	{
		if ($value === NULL) return NULL;
		switch ($type = pg_field_type($this->_result, is_numeric($column) ? $column : pg_field_num($this->_result, $column)))
		{
			case 'bool':
				if ($value == 't') return true;
				else if ($value == 'f') return false;
				else return (bool)$value;
			case 'int2':
			case 'int4':
			case 'int8':
				return (int)$value;
			case 'float2':
			case 'float4':
			case 'float8':
			case 'numeric':
				return (float)$value;
		}
		return $value;
	}

	protected function fixTypes($row)
	{
		if (is_array($row))
		{
			$i = 0;
			foreach ($row as $column => $val) $row[$column] = $this->fixType($val, $i++);
		}
		else if (is_object($row))
		{
			$i = 0;
			foreach ($row as $column => $val) $row->{$column} = $this->fixType($val, $i++);
		}
		return $row;
	}

	public function as_array($key = NULL, $value = NULL)
	{
		if ($this->_total_rows === 0)
			return array();

		if ( ! $this->_as_object AND $key === NULL)
		{
			// Rewind
			$this->_current_row = 0;

			if ($value === NULL)
			{
				// Indexed rows
				return array_map(array($this, 'fixTypes'), pg_fetch_all($this->_result));
			}

			// Indexed columns
			$t = $this;
			return array_map(function($val) use($t, $value) { return $t->fixType($val, $value); }, pg_fetch_all_columns($this->_result, pg_field_num($this->_result, $value)));
		}

		return parent::as_array($key, $value);
	}

	/**
	 * SeekableIterator: seek
	 */
	public function seek($offset)
	{
		if ( ! $this->offsetExists($offset))
			return FALSE;

		$this->_current_row = $offset;

		return TRUE;
	}

	/**
	 * Iterator: current
	 */
	public function current()
	{
		if ( ! $this->offsetExists($this->_current_row))
			return FALSE;

		if ( ! $this->_as_object)
			return $this->fixTypes(pg_fetch_assoc($this->_result, $this->_current_row));

		if ( ! $this->_object_params)
			return $this->fixTypes(pg_fetch_object($this->_result, $this->_current_row, $this->_as_object));

		return $this->fixTypes(pg_fetch_object($this->_result, $this->_current_row, $this->_as_object, $this->_object_params));
	}

}