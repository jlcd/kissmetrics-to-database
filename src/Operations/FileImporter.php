<?php

namespace KissmetricsToDatabase\Operations;

use PDO;
use SplFileInfo;

class FileImporter
{
    /**
     * @var PDO $client
     */ 
    private $client;

    /**
     * @var boolean $isDirty
     */
    private $isDirty = false;

    /**
     * @var array $columns
     */
    private $columns = [];

    /**
     * @param PDO $client
     */
    public function __construct(PDO $client)
    {
        $this->client = $client;

        $statement = $client->prepare(
            'SELECT column_name FROM information_schema.columns
            WHERE table_name = \'tb_event\''
        );
        $statement->execute();

        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->columns[] = $row['column_name'];
        }
    }

    /**
     * @param array $row
     * @param boolean $isDirty
     */
    private function expandTable(array $row)
    {
        $possibleColumns = array_keys($row);
        $newColumns = array_diff($possibleColumns, $this->columns);

        if ($newColumns && $this->isDirty) {
            $this->client->commit();
        }
        foreach ($newColumns as $newColumn) {
            $statement = $this->client->prepare(
                "ALTER TABLE tb_event ADD $newColumn VARCHAR(max)"
            );
            $statement->execute();

            $this->columns[] = $newColumn;
        }

        if ($newColumns && $this->isDirty) {
            $this->client->beginTransaction();
            $this->isDirty = false;
        }
    }

    /**
     * @param array $row
     */
    private function persist(array $row)
    {
        $columns = array_keys($row);
        $binds = array_map(function ($c) {
            return '?';
        }, $columns);

        $sql = sprintf(
            'INSERT INTO tb_event (created_at, %s) VALUES (\'%s\', %s)',
            implode(',', $columns),
            (new \DateTime())->format('Y-m-d'),
            implode(',', $binds)
        );

        $statement = $this->client->prepare($sql);
        $statement->execute(array_values($row));
    }

    /**
     * @param array $row
     */
    private function identify(array $row)
    {
        $id = $row['_p'];
        if (!array_key_exists('_p2', $row)) {
            $alias = $id;
        } else {
            $alias = $row['_p2'];
        }

        $statement = $this->client->prepare(
            'SELECT id FROM tb_identity
            WHERE alias IN (:id, :alias)
            LIMIT 1'
        );
        $statement->bindValue(':id', $id);
        $statement->bindValue(':alias', $alias);
        $statement->execute();

        $existant = $statement->fetch(PDO::FETCH_ASSOC);

        $identifier = $id;
        if (false !== $existant) {
            $identifier = $existant['id'];
        }

        // This is the first time that I saw this user
        $statement = $this->client->prepare(
            'INSERT INTO tb_identity VALUES (?, ?)'
        );

        foreach ([$id, $alias] as $a) {
            if ($existant && $a == $identifier) {
                continue; // user already identified in the db
            }
            $statement->execute([$identifier, $a]);
        }
    }

    /**
     * @param SplFileInfo $file
     * @return boolean
     */
    public function from(SplFileInfo $file)
    {
        $processor = new ProcessFile($file);

        // begin a new session
        $this->client->beginTransaction();

        foreach ($processor->row() as $row) {
            // verify if the table needs to be expanded
            $this->expandTable($row);

            // persist the new row into the database
            $this->persist($row);

            // identify the user
            $this->identify($row);

            // Mark the session as dirty
            $this->isDirty = true;
        }

        // persist the session
        $this->client->commit();

        // mark the session as clean
        $this->isDirty = false;
    }
}

