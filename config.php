<?php
/**
 * Configure the application container using Pimple
 */
$container = new Pimple\Container();
$container['command.load-events'] = function ($container) {
    return new KissmetricsToDatabase\Commands\LoadEventsCommand();
};

/**
 * THIS FILE MUST RETURN THE PIMPLE CONTAINER
 */
return $container;

