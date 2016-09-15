<?php
/**
 * Configure the application container using Pimple
 */
$container = new Pimple\Container();

$container['aws.s3.client'] = $container->factory(function () {
    return new Aws\S3\S3Client([
        'region' => 'us-east-1',
        'credentials' => [
            'key' => getenv('AWS_ACCESS_KEY_ID'),
            'secret' => getenv('AWS_SECRET_ACCESS_KEY')
        ],
        'version' => 'latest',
    ]);
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

    $client = new PDO($dsn);
    $client->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    return $client;
});

$container['operations.s3-sync'] = $container->factory(function ($c) {
    return new KissmetricsToDatabase\Operations\SyncBucket(
        $c['aws.s3.client'],
        [
            'source' => getenv('AWS_S3_BUCKET'),
            'destination' => getenv('FILES_DIR')
        ]
    );
});

$container['operations.file-importer'] = $container->factory(
    function ($c) {
        return new KissmetricsToDatabase\Operations\FileImporter(
            $c['redshift.client']
        );
    }
);

$container['command.db-create'] = $container->factory(function () {
    return new KissmetricsToDatabase\Commands\CreateDatabaseCommand();
});

$container['command.process-files'] = $container->factory(function ($c) {
    return new KissmetricsToDatabase\Commands\ProcessFilesCommand([
        'work_dir' => getenv('WORK_DIR'),
        'files_dir' => getenv('FILES_DIR'),
        'last_read_file' => getenv('LAST_READ_FILE'),
    ]);

});

/**
 * THIS FILE MUST RETURN THE PIMPLE CONTAINER
 */
return $container;

