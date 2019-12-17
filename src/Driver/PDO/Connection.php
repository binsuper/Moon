<?php

namespace Moon\Driver\PDO;

class Connection implements \Moon\Core\Connection\ConnectionInterface {

    private string $__dsn;

    private string $__ip;

    private string $__user;

    /**
     * Connection constructor.
     * @param array $options
     */
    public function __construct(array $options) {

    }

    public function connect() {

    }

    public function execCommand() {
        // TODO: Implement execCommand() method.
    }
}