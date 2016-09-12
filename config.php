<?php
/**
 * Configure the application container using Pimple
 */
$container = new Pimple\Container();
$container['aws.s3.client'] = function($container) {
    return new Aws\S3\S3MultiRegionClient(['version' => 'latest']);
};
$container['operations.sync-s3-bucket'] = function ($container) {
    return new KissmetricsToDatabase\Operations\SyncBucket(
        $container['aws.s3.client'],
        [
            'source' => getenv('AWS_S3_BUCKET'),
            'destination' => getenv('WORK_DIR')
        ]
    );
};
$container['command.load-events'] = function ($container) {
    $command = new KissmetricsToDatabase\Commands\LoadEventsCommand([
        'operations' => [
            'sync-s3-bucket' => $container['operations.sync-s3-bucket']
        ]
    ]);

    return $command;
};

/**
 * THIS FILE MUST RETURN THE PIMPLE CONTAINER
 */
return $container;

