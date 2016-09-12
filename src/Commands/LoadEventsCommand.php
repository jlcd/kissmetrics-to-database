<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class LoadEventsCommand extends Command
{
    /**
     * @var array $operations
     */
    private $operations;

    /**
     * @param string $name
     * @param OperationInterface $operation
     * 
     * @return $this
     */
    public function addOperation($name, OperationInterface $operation)
    {
        $this->operations[$name] = $operation;

        return $this;
    }

    protected function configure()
    {
        $this->setName('load-events')
            ->setDescription('Load all events into the database');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        // syncronize the local directory with the S3 Bucket
        // that contains all files from Kiss Metrics
        $syncOp = $this->operations['s3-sync'];
        $syncOp->execute();

        // list of operations to be executed (in order of execution)
        $operationsToExecute = [
            'sync-s3-bucket',
        ];

        foreach ($operationsToExecute as $operation)
        $output->writeln('Olá');
    }
}

