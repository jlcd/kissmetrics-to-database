<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class LoadEventsCommand extends Command
{
    protected function configure()
    {
        $this->setName('load-events')
            ->setDescription('Load all events into the database');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->writeln('Olá');
    }
}

