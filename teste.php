<?php
$startTime = microtime();

$sql = <<<SQL
CREATE TABLE tb_identity (
    id INTEGER NOT NULL PRIMARY KEY,
    nicks TEXT NOT NULL
);
SQL;

$database = new PDO(
    'sqlite::memory:',
    null,
    null,
    [PDO::ATTR_PERSISTENT => true]
);

try {
    echo 'Creating the temporary database...' . PHP_EOL;
    $database->exec($sql);
} catch (PDOException $e) {
    echo 'Error while creating the temporary database: ';
    echo $e->getMessage();
    echo PHP_EOL;
    exit(1);
}

$file = fopen('book.csv', 'r');
$headers = null;
$lines = 0;
$counter = 0;

while ($data = fgetcsv($file, 0, ';')) {
    $lines++;

    if (is_null($headers)) {
        $headers = $data;
        continue;
    }

    $row = array_combine($headers, $data);
    
    $id = trim($row['_p']);
    $nick = trim($row['_p2']);

    if (empty($nick)) {
        // Insert a new registry
        $statement = $database->prepare(
            'INSERT INTO tb_identity (nicks) VALUES (:nick)'
        );
        $statement->bindValue(':nick', $id);
        $statement->execute();
        $counter++;

        continue;
    }

    $statement = $database->prepare(
        'SELECT id, nicks FROM tb_identity
        WHERE nicks LIKE :id OR nicks LIKE :nick'
    );
    $statement->bindValue(':nick', '%' . $nick . '%');
    $statement->bindValue(':id', '%' . $id . '%');
    $statement->execute();

    $row = $statement->fetch(PDO::FETCH_ASSOC);
    $nicks = [];
    if (false !== $row) {
        // Update the existing list of nicks
        $nicks = explode('|', $row['nicks']);
        $nicks[] = $id;
        if ($nicks) {
            $nicks[] = $nick;
        }

        $statement = $database->prepare(
            'UPDATE tb_identity SET nicks = :nick WHERE id = :id'
        );
        $statement->bindValue(':id', $row['id']);
    } else {
        // Insert the new combination of id and nick
        $nicks = [$id, $nick];
        $statement = $database->prepare(
            'INSERT INTO tb_identity (nicks) VALUES (:nick)'
        );

        $counter++;
    }

    $statement->bindValue(':nick', implode('|', array_unique($nicks)));
    $statement->execute();

    if ($statement->rowCount() == 0) {
        echo 'Error while saving identity into the database' . PHP_EOL;
        var_dump($statement->errorInfo());
        exit(1);
    }
}

$bytesUsed = memory_get_peak_usage() / 1024;
$timeElapsed = abs(microtime() - $startTime);

echo PHP_EOL;
echo 'Number of lines processed: ' . $lines . PHP_EOL;
echo 'Number of rows inserted: ' . $counter . PHP_EOL;
echo 'Time elapsed (in seconds): ' . $timeElapsed . PHP_EOL;
echo 'Max KBs used: ' . $bytesUsed . PHP_EOL;
echo PHP_EOL;

exit(0);

$statement = $database->query(
    'SELECT id, nicks FROM tb_identity ORDER BY id ASC'
);

while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
    $nicks = explode('|', $row['nicks']);
    if (2 >= count($nicks)) continue;
    echo 'ID: ' . $row['id'] . ' has ' . count($nicks) . ' nicks associated.' . PHP_EOL;
    foreach ($nicks as $nick) {
        echo '    ' . $nick . PHP_EOL;
    }

    echo PHP_EOL;
}

