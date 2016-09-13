<?php

namespace KissmetricsToDatabase\Operations;

use PDO;
use SplFileInfo;

class RedShiftImporter
{
    /**
     * @var PDO $client
     */ 
    private $client;

    /**
     * @var string $tableName
     */
    private $tableName;

    /**
     * @var string $fieldType
     */
    private $fieldType;

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

        $statament = $client->prepare(
            'SELECT column_name FROM information_schema.columns
            WHERE table_name = tb_event'
        );
        $statement->execute();

        $rows = $statement->fetch(PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->columns[] = $row['column_name'];
        }
    }

    /**
     * @param array $row
     */
    private function expandTable(array $row)
    {
        $possibleColumns = array_keys($row);
        $newColumns = array_diff($possibleColumns, $this->columns);
        foreach ($newColumns as $newColumn) {
            $statement = $this->client->prepare(
                'ALTER TABLE tb_event ADD :column TEXT'
            );
            $statament->bindValue(':column', $newColumn);
            $statament->execute();

            $this->columns[] = $newColumn;
        }
    }

    /**
     * @param array $row
     */
    private function persist(array $row)
    {
        $columns = array_keys($row);
        $binds = array_map(function ($c) {
            return ':' . $c;
        });

        $sql = sprintf(
            'INSERT INTO tb_event (%s) VALUES (%s)',
            implode(',', $columns),
            implode(',', $binds)
        );

        $statement = $this->client->prepare($sql);
        foreach ($row as $column => $value) {
            $statement->bindValue(':' . $column, $value);
        }
        $statement->execute();
    }

    /**
     * @param SplFileInfo $file
     * @return boolean
     */
    public function importFrom(SplFileInfo $file)
    {
        try {
            $processor = ProcessFile($file);

            // begin a new session
            $this->client->beginTransaction();

            foreach ($processor->row() as $row) {
                // verify if the table needs to be expanded
                $this->expandTable($row);

                // persist the new row into the database
                $this->persist($row);
            }

            // persist the session
            $this->client->commit();

            return true;
        } catch (Exception $e) {
            $this->client->rollback();
            // TODO: Add a log here to debug the possible exceptions
        }

        // Problem importing this file
        return false;
    }
}

