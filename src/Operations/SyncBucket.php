<?php

namespace KissmetricsToDatabase\Operations;

use Aws\S3\S3ClientInterface;
use Aws\S3\Transfer;

class SyncBucket
{
    /**
     * @var S3Client $client
     */
    private $client;

    /**
     * @var string $source
     */
    private $source;

    /**
     * @var string $destination
     */
    private $destination;

    public function __construct(S3ClientInterface $client, array $options = [])
    {
        $this->client = $client;
        if (array_key_exists('source', $options)) {
            $this->source = $options['source'];
        }
        if (array_key_exists('destination', $options)) {
            $this->destination = $options['destination'];
        }
    }

    public function sync()
    {
        $transfer = new Transfer(
            $this->client,
            $this->source,
            $this->destination
        );
        $transfer->transfer();
    }
}

