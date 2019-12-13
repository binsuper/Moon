<?php


namespace Moon\Core\DB;


use Moon\Core\Error\InvalidArgumentException;

class Raw {

    protected $_value;
    protected array $_map;

    public function __construct($value = null, array $map = []) {
        $this->_value = $value;
        $this->_map = $map;
    }

    /**
     * @return string|Selector
     */
    public function value() {
        return $this->_value;
    }

    /**
     * @return array
     */
    public function map(): array {
        return $this->_map;
    }


}