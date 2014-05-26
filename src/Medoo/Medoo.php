<?php

/**
 * Medoo database framework
 *
 * @link http://medoo.in
 * @copyright Angel Lai 2014
 * @license MIT
 * @version 0.9.5.3
 */

namespace Medoo;

class Medoo
{
	/**
	 * @var string
	 */
	protected $queryString;

	/**
	 * Constructor
	 *
	 * @param \PDO $pdo
	 */
	public function __construct($pdo = null)
	{
		null !== $pdo && $this->pdo = $pdo;
	}

	/**
	 * Passthru pdo->query()
	 *
	 * @param string $query
	 * @return \PDOStatement
	 */
	public function query($query)
	{
		$this->queryString = $query;
		return $this->pdo->query($query);
	}

	/**
	 * Passthru pdo->exec()
	 *
	 * @param string $query
	 * @return \PDOStatement
	 */
	public function exec($query)
	{
		$this->queryString = $query;
		return $this->pdo->exec($query);
	}

	/**
	 * Passthru pdo->quote()
	 *
	 * @param string $string
	 * @return string
	 */
	public function quote($string)
	{
		return $this->pdo->quote($string);
	}

	protected function columnQuote($string)
	{
		return '"' . str_replace('.', '"."', $string) . '"';
	}

	protected function columnPush($columns)
	{
		if ($columns == '*'){
			return $columns;
		}

		if (is_string($columns)){
			$columns = array($columns);
		}

		$stack = array();

		foreach ($columns as $key => $value){
			preg_match('/([a-zA-Z0-9_\-\.]*)\s*\(([a-zA-Z0-9_\-]*)\)/i', $value, $match);

			if (isset($match[1], $match[2])){
				array_push($stack, $this->columnQuote( $match[1] ) . ' AS ' . $this->columnQuote( $match[2] ));
			} else {
				array_push($stack, $this->columnQuote( $value ));
			}
		}

		return implode($stack, ',');
	}

	protected function array_quote($array)
	{
		$temp = array();
		foreach ($array as $value){
			$temp[] = is_int($value) ? $value : $this->quote($value);
		}
		return implode($temp, ',');
	}

	protected function inner_conjunct($data, $conjunctor, $outer_conjunctor)
	{
		$haystack = array();
		foreach ($data as $value) {
			$haystack[] = '(' . $this->data_implode($value, $conjunctor) . ')';
		}
		return implode($outer_conjunctor . ' ', $haystack);
	}

	protected function data_implode($data, $conjunctor, $outer_conjunctor = null)
	{
		$wheres = array();

		foreach ($data as $key => $value){
			$type = gettype($value);

			if (preg_match('/^(AND|OR)\s*#?/i', $key, $matches) && $type == 'array') {
				$wheres[] = 0 !== count(array_diff_key($value, array_keys(array_keys($value)))) ?
					'(' . $this->data_implode($value, ' ' . $matches[1]) . ')' :
					'(' . $this->inner_conjunct($value, ' ' . $matches[1], $conjunctor) . ')';
			} else {
				preg_match('/([\w\.]+)(\[(\>|\>\=|\<|\<\=|\!|\<\>|\>\<)\])?/i', $key, $match);
				$column = $this->columnQuote($match[1]);

				if (isset($match[3])) {
					if ($match[3] == '') {
						$wheres[] = $column . ' ' . $match[3] . '= ' . $this->quote($value);
					} elseif ($match[3] == '!') {
						switch ($type) {
							case 'NULL':
								$wheres[] = $column . ' IS NOT NULL';
								break;

							case 'array':
								$wheres[] = $column . ' NOT IN (' . $this->array_quote($value) . ')';
								break;

							case 'integer':
							case 'double':
								$wheres[] = $column . ' != ' . $value;
								break;

							case 'string':
								$wheres[] = $column . ' != ' . $this->quote($value);
								break;
						}
					} else {
						if ($match[3] == '<>' || $match[3] == '><') {
							if ($type == 'array') {
								if ($match[3] == '><') {
									$column .= ' NOT';
								}

								if (is_numeric($value[0]) && is_numeric($value[1])) {
									$wheres[] = '(' . $column . ' BETWEEN ' . $value[0] . ' AND ' . $value[1] . ')';
								} else {
									$wheres[] = '(' . $column . ' BETWEEN ' . $this->quote($value[0]) . ' AND ' . $this->quote($value[1]) . ')';
								}
							}
						} else {
							if (is_numeric($value)) {
								$wheres[] = $column . ' ' . $match[3] . ' ' . $value;
							} else {
								$datetime = strtotime($value);

								if ($datetime) {
									$wheres[] = $column . ' ' . $match[3] . ' ' . $this->quote(date('Y-m-d H:i:s', $datetime));
								}
							}
						}
					}
				} else {
					if (is_int($key)) {
						$wheres[] = $this->quote($value);
					} else {
						switch ($type) {
							case 'NULL':
								$wheres[] = $column . ' IS NULL';
								break;

							case 'array':
								$wheres[] = $column . ' IN (' . $this->array_quote($value) . ')';
								break;

							case 'integer':
							case 'double':
								$wheres[] = $column . ' = ' . $value;
								break;

							case 'boolean':
								$wheres[] = $column . ' = ' . ($value ? '1' : '0');
								break;

							case 'string':
								$wheres[] = $column . ' = ' . $this->quote($value);
								break;
						}
					}
				}
			}
		}

		return implode($conjunctor . ' ', $wheres);
	}

	protected function where_clause($where)
	{
		$where_clause = '';

		if (is_array($where)) {
			$where_keys = array_keys($where);
			$where_AND = preg_grep("/^AND\s*#?$/i", $where_keys);
			$where_OR = preg_grep("/^OR\s*#?$/i", $where_keys);

			$single_condition = array_diff_key($where, array_flip(
				explode(' ', 'AND OR GROUP ORDER HAVING LIMIT LIKE MATCH')
			));

			if ($single_condition != array()) {
				$where_clause = ' WHERE ' . $this->data_implode($single_condition, '');
			}

			if (!empty($where_AND)) {
				$value = array_values($where_AND);
				$where_clause = ' WHERE ' . $this->data_implode($where[ $value[0] ], ' AND');
			}

			if (!empty($where_OR)) {
				$value = array_values($where_OR);
				$where_clause = ' WHERE ' . $this->data_implode($where[ $value[0] ], ' OR');
			}

			if (isset($where['LIKE'])) {
				$like_query = $where['LIKE'];

				if (is_array($like_query)) {
					$is_OR = isset($like_query['OR']);
					$clause_wrap = array();

					if ($is_OR || isset($like_query['AND'])) {
						$connector = $is_OR ? 'OR' : 'AND';
						$like_query = $is_OR ? $like_query['OR'] : $like_query['AND'];
					} else {
						$connector = 'AND';
					}

					foreach ($like_query as $column => $keyword) {
						if (is_array($keyword)) {
							foreach ($keyword as $key) {
								$clause_wrap[] = $this->columnQuote($column) . ' LIKE ' . $this->quote('%' . $key . '%');
							}
						} else {
							$clause_wrap[] = $this->columnQuote($column) . ' LIKE ' . $this->quote('%' . $keyword . '%');
						}
					}

					$where_clause .= ($where_clause != '' ? ' AND ' : ' WHERE ') . '('
						. implode($clause_wrap, ' ' . $connector . ' ') . ')';
				}
			}

			if (isset($where['MATCH'])) {
				$match_query = $where['MATCH'];

				if (is_array($match_query) && isset($match_query['columns'], $match_query['keyword'])) {
					$where_clause .= ($where_clause != '' ? ' AND ' : ' WHERE ') . ' MATCH ("'
						. str_replace('.', '"."', implode($match_query['columns'], '", "')) . '") AGAINST ('
						. $this->quote($match_query['keyword']) . ')';
				}
			}

			if (isset($where['GROUP'])) {
				$where_clause .= ' GROUP BY ' . $this->columnQuote($where['GROUP']);
			}

			if (isset($where['ORDER'])) {
				if (is_array($where['ORDER'])) {
					$where_clause .= ' ORDER BY FIELD(' . $this->columnQuote($where['ORDER'][0]) . ', '
						. $this->array_quote($where['ORDER'][1]) . ')';
				} else {
					preg_match('/(^[a-zA-Z0-9_\-\.]*)(\s*(DESC|ASC))?/', $where['ORDER'], $order_match);

					$where_clause .= ' ORDER BY "' . str_replace('.', '"."', $order_match[1]) . '" '
					. (isset($order_match[3]) ? $order_match[3] : '');
				}

				if (isset($where['HAVING'])) {
					$where_clause .= ' HAVING ' . $this->data_implode($where['HAVING'], '');
				}
			}

			if (isset($where['LIMIT'])) {
				if (is_numeric($where['LIMIT'])) {
					$where_clause .= ' LIMIT ' . $where['LIMIT'];
				}

				if (is_array($where['LIMIT']) && is_numeric($where['LIMIT'][0]) && is_numeric($where['LIMIT'][1])) {
					$where_clause .= ' LIMIT ' . $where['LIMIT'][0] . ',' . $where['LIMIT'][1];
				}
			}
		}
		else
		{
			if ($where != null)
			{
				$where_clause .= ' ' . $where;
			}
		}

		return $where_clause;
	}

	protected function select_context($table, $join, &$columns = null, $where = null, $column_fn = null)
	{
		$table = '"' . $table . '"';
		$join_key = is_array($join) ? array_keys($join) : null;

		if (
			isset($join_key[0]) &&
			strpos($join_key[0], '[') === 0
		)
		{
			$table_join = array();

			$join_array = array(
				'>' => 'LEFT',
				'<' => 'RIGHT',
				'<>' => 'FULL',
				'><' => 'INNER'
			);

			foreach($join as $sub_table => $relation)
			{
				preg_match('/(\[(\<|\>|\>\<|\<\>)\])?([a-zA-Z0-9_\-]*)/', $sub_table, $match);

				if ($match[2] != '' && $match[3] != '')
				{
					if (is_string($relation))
					{
						$relation = 'USING ("' . $relation . '")';
					}

					if (is_array($relation))
					{
						// For ['column1', 'column2']
						if (isset($relation[0]))
						{
							$relation = 'USING ("' . implode($relation, '", "') . '")';
						}
						// For ['column1' => 'column2']
						else
						{
							$relation = 'ON ' . $table . '."' . key($relation) . '" = "' . $match[3] . '"."' . current($relation) . '"';
						}
					}

					$table_join[] = $join_array[ $match[2] ] . ' JOIN "' . $match[3] . '" ' . $relation;
				}
			}

			$table .= ' ' . implode($table_join, ' ');
		}
		else
		{
			if (is_null($columns))
			{
				if (is_null($where))
				{
					if (
						is_array($join) &&
						isset($column_fn)
					)
					{
						$where = $join;
						$columns = null;
					}
					else
					{
						$where = null;
						$columns = $join;
					}
				}
				else
				{
					$where = $join;
					$columns = null;
				}
			}
			else
			{
				$where = $columns;
				$columns = $join;
			}
		}

		if (isset($column_fn))
		{
			if ($column_fn == 1)
			{
				$column = '1';

				if (is_null($where))
				{
					$where = $columns;
				}
				else
				{
					$where == $join;
				}
			}
			else
			{
				if (empty($columns))
				{
					$columns = '*';
					$where = $join;
				}

				$column = $column_fn . '(' . $this->columnPush($columns) . ')';
			}
		}
		else
		{
			$column = $this->columnPush($columns);
		}

		return 'SELECT ' . $column . ' FROM ' . $table . $this->where_clause($where);
	}

	public function select($table, $join, $columns = null, $where = null)
	{
		$query = $this->query($this->select_context($table, $join, $columns, $where));

		return $query ? $query->fetchAll(
			(is_string($columns) && $columns != '*') ? PDO::FETCH_COLUMN : PDO::FETCH_ASSOC
		) : false;
	}

	public function insert($table, $datas)
	{
		$lastId = array();

		// Check indexed or associative array
		if (!isset($datas[0]))
		{
			$datas = array($datas);
		}

		foreach ($datas as $data)
		{
			$keys = array_keys($data);
			$values = array();
			$index = 0;

			foreach ($data as $key => $value)
			{
				switch (gettype($value))
				{
					case 'NULL':
						$values[] = 'NULL';
						break;

					case 'array':
						preg_match("/\(JSON\)\s*([\w]+)/i", $key, $column_match);

						if (isset($column_match[0]))
						{
							$keys[ $index ] = $column_match[1];
							$values[] = $this->quote(json_encode($value));
						}
						else
						{
							$values[] = $this->quote(serialize($value));
						}
						break;

					case 'boolean':
						$values[] = ($value ? '1' : '0');
						break;

					case 'integer':
					case 'double':
					case 'string':
						$values[] = $this->quote($value);
						break;
				}

				$index++;
			}

			$this->exec('INSERT INTO "' . $table . '" ("' . implode('", "', $keys) . '") VALUES (' . implode($values, ', ') . ')');

			$lastId[] = $this->pdo->lastInsertId();
		}

		return count($lastId) > 1 ? $lastId : $lastId[ 0 ];
	}

	public function update($table, $data, $where = null)
	{
		$fields = array();

		foreach ($data as $key => $value)
		{
			preg_match('/([\w]+)(\[(\+|\-|\*|\/)\])?/i', $key, $match);

			if (isset($match[3]))
			{
				if (is_numeric($value))
				{
					$fields[] = $this->columnQuote($match[1]) . ' = ' . $this->columnQuote($match[1]) . ' ' . $match[3] . ' ' . $value;
				}
			}
			else
			{
				$column = $this->columnQuote($key);

				switch (gettype($value))
				{
					case 'NULL':
						$fields[] = $column . ' = NULL';
						break;

					case 'array':
						preg_match("/\(JSON\)\s*([\w]+)/i", $key, $column_match);

						if (isset($column_match[0]))
						{
							$fields[] = $this->columnQuote($column_match[1]) . ' = ' . $this->quote(json_encode($value));
						}
						else
						{
							$fields[] = $column . ' = ' . $this->quote(serialize($value));
						}
						break;

					case 'boolean':
						$fields[] = $column . ' = ' . ($value ? '1' : '0');
						break;

					case 'integer':
					case 'double':
					case 'string':
						$fields[] = $column . ' = ' . $this->quote($value);
						break;
				}
			}
		}

		return $this->exec('UPDATE "' . $table . '" SET ' . implode(', ', $fields) . $this->where_clause($where));
	}

	public function delete($table, $where)
	{
		return $this->exec('DELETE FROM "' . $table . '"' . $this->where_clause($where));
	}

	public function replace($table, $columns, $search = null, $replace = null, $where = null)
	{
		if (is_array($columns))
		{
			$replace_query = array();

			foreach ($columns as $column => $replacements)
			{
				foreach ($replacements as $replace_search => $replace_replacement)
				{
					$replace_query[] = $column . ' = REPLACE("' . $column . '", ' . $this->quote($replace_search) . ', ' . $this->quote($replace_replacement) . ')';
				}
			}

			$replace_query = implode(', ', $replace_query);
			$where = $search;
		}
		else
		{
			if (is_array($search))
			{
				$replace_query = array();

				foreach ($search as $replace_search => $replace_replacement)
				{
					$replace_query[] = $columns . ' = REPLACE("' . $columns . '", ' . $this->quote($replace_search) . ', ' . $this->quote($replace_replacement) . ')';
				}

				$replace_query = implode(', ', $replace_query);
				$where = $replace;
			}
			else
			{
				$replace_query = $columns . ' = REPLACE("' . $columns . '", ' . $this->quote($search) . ', ' . $this->quote($replace) . ')';
			}
		}

		return $this->exec('UPDATE "' . $table . '" SET ' . $replace_query . $this->where_clause($where));
	}

	public function get($table, $columns, $where = null)
	{
		if (!isset($where))
		{
			$where = array();
		}

		$where['LIMIT'] = 1;

		$data = $this->select($table, $columns, $where);

		return isset($data[0]) ? $data[0] : false;
	}

	public function has($table, $join, $where = null)
	{
		$column = null;

		return $this->query('SELECT EXISTS(' . $this->select_context($table, $join, $column, $where, 1) . ')')->fetchColumn() === '1';
	}

	public function count($table, $join = null, $column = null, $where = null)
	{
		return 0 + ($this->query($this->select_context($table, $join, $column, $where, 'COUNT'))->fetchColumn());
	}

	public function max($table, $join, $column = null, $where = null)
	{
		$max = $this->query($this->select_context($table, $join, $column, $where, 'MAX'))->fetchColumn();

		return is_numeric($max) ? $max + 0 : $max;
	}

	public function min($table, $join, $column = null, $where = null)
	{
		$min = $this->query($this->select_context($table, $join, $column, $where, 'MIN'))->fetchColumn();

		return is_numeric($min) ? $min + 0 : $min;
	}

	public function avg($table, $join, $column = null, $where = null)
	{
		return 0 + ($this->query($this->select_context($table, $join, $column, $where, 'AVG'))->fetchColumn());
	}

	public function sum($table, $join, $column = null, $where = null)
	{
		return 0 + ($this->query($this->select_context($table, $join, $column, $where, 'SUM'))->fetchColumn());
	}

	public function error()
	{
		return $this->pdo->errorInfo();
	}

	public function last_query()
	{
		return $this->queryString;
	}

	public function info()
	{
		$output = array(
			'server' => 'SERVER_INFO',
			'driver' => 'DRIVER_NAME',
			'client' => 'CLIENT_VERSION',
			'version' => 'SERVER_VERSION',
			'connection' => 'CONNECTION_STATUS'
		);

		foreach ($output as $key => $value)
		{
			$output[ $key ] = $this->pdo->getAttribute(constant('PDO::ATTR_' . $value));
		}

		return $output;
	}
}
