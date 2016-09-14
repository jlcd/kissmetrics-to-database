<?php

namespace KissmetricsToDatabase\Commands;

use PDO;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class ProcessIdentitiesCommand extends Command
{
    /**
     * @var PDO $client
     */
    private $client;

    protected function configure()
    {
        $this->setName('process-identities')
            ->setDescription('Process the list of duplicated entities');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {

    }
}

