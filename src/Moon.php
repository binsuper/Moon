<?php


namespace Moon;


use Moon\Core\DB\Raw;

class Moon {

    private function __construct() {
    }

    public static function loadCfg() {
    }

    public static function handle($handler) {

        // $conn = $this->connector->getConnection($handler);

        // $constructor = $this->getConstructor($this->cfg, $handler);

        // $result = $conn->execCommand($constructor);

        // return $result;
    }

    /**
     * @param $val
     * @param array $map
     * @return Raw
     */
    public static function raw($val, array $map = []): Raw {
        return new Raw($val, $map);
    }

}