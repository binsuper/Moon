<?php


namespace DB;

class MoonConnection extends \Moon\MedooConnection{
    
    protected function logError(){
        if($this->isError()){
            $error = $this->error();
            $msg = sprintf('%s;[SQL]%s', ($error[2] ?? json_encode($error)), $this->last());
            logMessage($msg, LOG_ERR);
        }
    }
    
    protected function logFailed($ret){
        if($ret == false){
            $msg = sprintf('execute failed;[SQL]%s', $this->last(true) ?: '');
            logMessage($msg, LOG_ERR);
        }
    }
    
    
    
    public function delete(\Moon\Selector $selector) {
        $ret = parent::delete($selector);
        $this->logFailed($ret);
        $this->logError();
        return $ret;
    }

    public function fetch(\Moon\Selector $selector) {
        $ret =  parent::fetch($selector);
        $this->logError();
        return $ret;
    }

    public function fetchAll(\Moon\Selector $selector) {
        $ret =  parent::fetchAll($selector);
        $this->logError();
        return $ret;
    }

    public function insert(\Moon\Selector ...$selectors) {
        $ret =  parent::insert(...$selectors);
        $this->logFailed($ret);
        $this->logError();
        return $ret;
    }

    public function rowCount(\Moon\Selector $selector) {
        $ret =  parent::rowCount($selector);
        $this->logError();
        return $ret;
    }

    public function update(\Moon\Selector $selector) {
        $ret =  parent::update($selector);
        $this->logFailed($ret);
        $this->logError();
        return $ret;
    }

    
}
