<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class CreateDatabaseCommand extends Command
{
    protected function configure()
    {
        $this->setName('create-database')
            ->setDescription(
                'Create the table structures into a RedShift instance'
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $client = $this->getApplication()->make('redshift.client');
        $eventSql = 'CREATE TABLE IF NOT EXISTS tb_event (
            created_at DATE
        )';

        $client->exec($eventSql);

        $identitySql = <<<SQL
CREATE TABLE IF NOT EXISTS tb_identity (
    id TEXT,
    alias TEXT
)
SQL;
        $client->exec($identitySql);
    }
}

