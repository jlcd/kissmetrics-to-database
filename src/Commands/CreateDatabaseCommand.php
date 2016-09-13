<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class CreateDatabaseCommand extends Command
{
    protected function configure()
    {
        $this->setName('db:create')
            ->setDescription(
                'Create the table structures into a RedShift instance'
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
    }
}

