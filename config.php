<?php
/**
 * Configure the application container using Pimple
 */
$container = new Pimple\Container();

$container['aws.s3.client'] = $container->factory(function () {
    $credentials = new Aws\Credentials\Credentials(
        getenv('AWS_ACCESS_KEY_ID'),
        getenv('AWS_SECRET_ACCESS_KEY')
    );
    return new Aws\S3\S3MultiRegionClient([
        'credentials' => $credentials,
        'version' => 'latest'
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
            'destination' => getenv('KM_DIR')
        ]
    );
});

$container['operations.red-shift-importer'] = $container->factory(
    function ($c) {
        return new KissmetricsToDatabase\Operations\RedShiftImporter(
            $c['redshift.client']
        );
    }
);

$container['command.load-events'] = $container->factory(function ($c) {
    $command = new KissmetricsToDatabase\Commands\LoadEventsCommand(
        [
            'work_dir' => getenv('WORK_DIR'),
            'km_dir' => getenv('KM_DIR'),
            'last_read_file' => getenv('LAST_READ_FILE'),
        ]
    );

    return $command;
});

$container['command.db-create'] = $container->factory(function () {
    return new KissmetricsToDatabase\Commands\CreateDatabaseCommand();
});

$container['command.process-identities'] = $container->factory(function () {
    return new KissmetricsToDatabase\Commands\ProcessIdentitiesCommand();
});

/**
 * THIS FILE MUST RETURN THE PIMPLE CONTAINER
 */
return $container;

