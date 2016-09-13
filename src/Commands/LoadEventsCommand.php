<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Finder\Finder;
use KissmetricsToDatabase\Operations\SyncBucket;
use KissmetricsToDatabase\Operations\RedShiftImporter;

class LoadEventsCommand extends Command
{
    /**
     * @var SyncBucket $syncBucket
     */
    private $syncBucket;

    /**
     * @var RedShiftImporter $importer
     */
    private $importer;

    /**
     * @var array $directories
     */
    private $directories;

    /**
     * @param array $directories
     */
    public function __construct(
        RedShiftImporter $importer,
        SyncBucket $syncBucket,
        array $directories
    )
    {
        parent::__construct();

        $this->importer = $importer;
        $this->syncBucket = $syncBucket;
        $this->directories = $directories;
    }

    protected function configure()
    {
        $this->setName('load-events')
            ->setDescription('Load all events into the database');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->writeln('Sync our local folder with the S3 Bucket');
        $this->syncBucket->sync();

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
                if (0 > strnatcmp($file->getFilename(), $lastFile)) {
                    return false;
                }
                return true;
            });
        }

        foreach ($finder as $file) {
            $loaded = $this->importer->importFrom($file);
            if ($loaded) {
                file_put_contents(
                    $this->directories['last_read_file'],
                    $file->getFilename()
                );
            }
        }
    }
}

