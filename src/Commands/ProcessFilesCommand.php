<?php

namespace KissmetricsToDatabase\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Finder\Finder;

class ProcessFilesCommand extends Command
{
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

    protected function configure()
    {
        $this->setName('process-files')
            ->setDescription('Process the new files available');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->writeln('Sync our local folder with the S3 Bucket...');
        $sync = $this->getApplication()->make('operations.s3-sync');
        $sync->sync();
        
        $output->writeln('Getting the list of files to be processed...');
        $finder = new Finder();
        $finder->files('*.json')
            ->in($this->directories['files_dir'])
            ->sort(function ($a, $b) {
                return strnatcmp($a->getRealPath(), $b->getRealPath());
            });

        if (file_exists($this->directories['last_read_file'])) {
            $lastFile = file_get_contents($this->directories['last_read_file']);
            $finder->filter(function ($file) use ($lastFile) {
                if (0 >= strnatcmp($file->getFilename(), $lastFile)) {
                    return false;
                }
                return true;
            });
        }

        $importer = $this->getApplication()
            ->make('operations.file-importer');
        foreach ($finder as $file) {
            $output->writeln(
                'Processing the events from ' . $file->getFilename() . '...'
            );
            
            $importer->from($file);

            file_put_contents(
                $this->directories['last_read_file'],
                $file->getFilename()
            );
        }
    }
}

