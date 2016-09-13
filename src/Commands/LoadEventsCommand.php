<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Finder\Finder;
use KissmetricsToDatabase\Operations\OperationInterface;

class LoadEventsCommand extends Command
{
    /**
     * @var array $operations
     */
    private $operations;

    /**
     * @var array $directories
     */
    private $directories;

    /**
     * @param array $directories
     */
    public function __construct(array $directories)
    {
        parent::__construct();

        $this->directories = $directories;
    }

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
        # $output->writeln('Sync our local folder with the S3 Bucket');
        # $syncOp = $this->operations['s3-sync'];
        # $syncOp->execute();

        $output->writeln('Getting the list of files to be processed...');
        $finder = new Finder();
        $finder->files('*.json')
            ->in($this->directories['km_dir'])
            ->sort(function ($a, $b) {
                return strnatcmp($a->getRealPath(), $b->getRealPath());
            });

        if (file_exists($this->directories['last_read_file'])) {
            $lastFile = file_get_contents($this->directories['last_read_file']);
            $finder->filter(function ($file) use ($lastFile) {
                if (0 > strnatcmp($file->getRealPath(), $lastFile)) {
                    return false;
                }
                return true;
            });
        }

        foreach ($finder as $file) {
            (new \KissmetricsToDatabase\Operations\ProcessFile())
                ->executeWithFile($file);
        }
    }
}

