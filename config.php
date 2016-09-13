<?php
/**
 * Configure the application container using Pimple
 */
$container = new Pimple\Container();

$container['aws.s3.client'] = $container->factory(function () {
    return new Aws\S3\S3MultiRegionClient(['version' => 'latest']);
});

$container['operations.sync-s3-bucket'] = $container->factory(function ($c) {
    return new KissmetricsToDatabase\Operations\SyncBucket(
        $c['aws.s3.client'],
        [
            'source' => getenv('AWS_S3_BUCKET'),
            'destination' => getenv('KM_DIR')
        ]
    );
});

$container['command.load-events'] = $container->factory(function ($c) {
    $command = new KissmetricsToDatabase\Commands\LoadEventsCommand(
        [
            'work_dir' => getenv('WORK_DIR'),
            'km_dir' => getenv('KM_DIR'),
            'last_read_file' => getenv('LAST_READ_FILE'),
        ]
    );
    $command->addOperation('sync-s3', $c['operations.sync-s3-bucket']);

    return $command;
});

$container['redshift.client'] = $container->factory(function () {
    $dsn = sprintf(
        'pgsql:host=%s;port=%d;dbname=%s;user=%s;password=%s',
        getenv('REDSHIFT_ENDPOINT'),
        getenv('REDSHIFT_PORT'),
        getenv('REDSHIFT_DBNAME'),
        getenv('REDSHIFT_USER'),
        getenv('REDSHIFT_PASSWORD')
    );

    return new PDO($dsn, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
});

/**
 * THIS FILE MUST RETURN THE PIMPLE CONTAINER
 */
return $container;

