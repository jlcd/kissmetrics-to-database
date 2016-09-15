<?php

namespace KissmetricsToDatabase;

use Symfony\Component\Console\Application as ConsoleApplication;
use Pimple\Container;

class Application extends ConsoleApplication
{
    /**
     * @var Container $container
     */
    private $container;

    /**
     * @param Container $container
     */
    public function __construct($name, $version, Container $c)
    {
        parent::__construct($name, $version);

        $this->container = $c;
    }

    /**
     * @param string $service
     * @return mixed
     */
    public function make($service)
    {
        return $this->container[$service];
    }
}

