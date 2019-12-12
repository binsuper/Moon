<?php

require_once 'vendor/autoload.php';

use Moon\Moon;
use Moon\Core\DB\Raw;
use Moon\Core\DB\Selector;
use Moon\Driver\PDO\Mysql\Constructor;

$subs = new Selector('class');
$subs->column(['t' => 'time[Int]']);
$subs->column(['p' => 'project[String]'])
    ->where('dd', 1);

$s = new Selector('user', 'u');

/*
$s->column('name[String], c, b(u)[Int]')
    ->column(Raw::column('count(1)'), 'cnt')
    ->columnSubquery($subs, 'cls')
    ->column([
        'info' => ['class', 'job', 'all' => []]
    ])
    ->columnInt('sex(ge)')
    ->column(Raw::column('user.*'));
*/

//$s->where('a', 'b')
//    ->whereBetween('u.b', Moon::raw('u.b + 1 and u.b + 10'))
//;
$s->join('class', ['id' => 'u.class_id', 'project' => 'u.project']);

$c = new Constructor(['prefix' => 'mob_'], $s);

print_r($c->selectContext());
//print_r($c->getDataMap());