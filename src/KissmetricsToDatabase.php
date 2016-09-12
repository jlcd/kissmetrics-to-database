<?php

namespace KissmetricsToDatabase;
use Dotenv;

class KissmetricsToDatabase
{
    private $local_s3_files                         = array();
    private $starting_read_local_s3_file            = null;
    private $last_read_local_s3_file                = null;
    private $output_group                           = 0;
    private $sanitized_keys                         = array();
    private $original_keys                          = array();
    private $db_conn                             	= null;
    private $db_stash_insert_values              	= array();
    private $base_insert_qry                        = null;
    private $total_local_s3_rows                    = null;
    private $exec_started                           = null;
    private $known_identities                       = null;
    private $known_identities_pairs                 = null;
    private $dead                                   = false;
    private $queriesData                            = 0;
    private $queriesDataIdentities                  = 0;
    private $start_datapoints_count                 = 0;
    private $end_datapoints_count                   = 0;
    private $start_identities_count                 = 0;
    private $end_identities_count                   = 0;


    /************
    *
        __construct
    *
    ************/
    public function __construct()
    {
        $this->exec_started = time();

        $dotenv = new Dotenv\Dotenv(dirname($_SERVER["SCRIPT_FILENAME"]));
        $dotenv->load();

        if (getenv('CFG_USE_JAVASCRIPT_OUTPUT') == "true" && !$this->isPhpCli()) {
            $this->createJsBaseScript();
        }

        echo "<body>";
        $this->output("Started: " . date('H:i:s'), true);

        if (getenv('CFG_USE_LOCK_FILE') == "true" && file_exists(getenv('CFG_LOCK_FILE'))) {
            $this->_die("Lock file found");
        }
        file_put_contents(getenv('CFG_LOCK_FILE'), time());

        $this->config();

        $current_count_qry = $this->databaseQuery("SELECT COUNT(1) as ct FROM " . getenv('DB_TABLE') );
        $current_count_res = $this->databaseGetResult($current_count_qry);
        $this->start_datapoints_count = $current_count_res[0]['ct'];

        $this->output("Start " . getenv('DB_TABLE') . " Count: " . $this->start_datapoints_count, true);

        if (getenv('DB_IDENTITIES_TABLE')) {
            $current_idcount_qry = $this->databaseQuery("SELECT COUNT(1) as ct FROM " . getenv('DB_IDENTITIES_TABLE') );
            $current_idcount_res = $this->databaseGetResult($current_idcount_qry);
            $this->start_identities_count = $current_idcount_res[0]['ct'];

            $this->output("Start " . getenv('DB_IDENTITIES_TABLE') . " Count: " . $this->start_identities_count, true);
        }
    }

    /************
    *
        __destruct
    *
    ************/
    public function __destruct()
    {
        $cleanup_title = ($this->dead ? "[DEAD] " : "")."Class Cleanup";
        $this->output($cleanup_title, true);

        $current_count_qry = $this->databaseQuery("SELECT COUNT(1) as ct FROM " . getenv('DB_TABLE') );
        $current_count_res = $this->databaseGetResult($current_count_qry);
        $this->end_datapoints_count = $current_count_res[0]['ct'];

        $count_difference = $this->end_datapoints_count - $this->start_datapoints_count;

        $this->output("End " . getenv('DB_TABLE') . " Count: " . $this->end_datapoints_count . " (+" . $count_difference . ")", true);


        if (getenv('DB_IDENTITIES_TABLE')) {
            $current_idcount_qry = $this->databaseQuery("SELECT COUNT(1) as ct FROM " . getenv('DB_IDENTITIES_TABLE') );
            $current_idcount_res = $this->databaseGetResult($current_idcount_qry);
            $this->end_identities_count = $current_idcount_res[0]['ct'];

            $idcount_difference = $this->end_identities_count - $this->start_identities_count;

            $this->output("End " . getenv('DB_IDENTITIES_TABLE') . " Count: " . $this->end_identities_count . " (+" . $idcount_difference . ")", true);
        }

        $this->output("Ended: " . date('H:i:s'), true);
        $this->output("Exec time: " . $this->getDateDiff(time(), $this->exec_started), true);

        if (!$this->dead) {
            if (getenv('CFG_USE_LOCK_FILE') == "true") {
                unlink(getenv('CFG_LOCK_FILE'));
            }
        }

        echo "</body>";

        if (getenv('CFG_EMAIL_SEND_OUTPUT_EMAIL_WHEN_PHP_CLI') == "true" && $this->isPhpCli()) {
            $output = ob_get_clean();
            $this->sendMail($output);
        }
    }

    /************
    *
        config
    *
    ************/
    private function config()
    {

        if (getenv('DB_MODIFY_CONNECT_TIMEOUT') == "true") {
            ini_set('mysql.connect_timeout', getenv('DB_MODIFY_CONNECT_TIMEOUT_TO'));
            ini_set('default_socket_timeout', getenv('DB_MODIFY_CONNECT_TIMEOUT_TO'));
        }

        if (getenv('CFG_SHOW_ERRORS') == "true") {
            error_reporting(E_ALL);
            ini_set('display_errors', 1);
        }

        set_time_limit(getenv('CFG_EXECUTION_TIME_LIMIT'));

        $this->starting_read_local_s3_file = $this->getLastReadLocalS3File();

        if (getenv('CFG_EMAIL_SEND_OUTPUT_EMAIL_WHEN_PHP_CLI') == "true" && $this->isPhpCli()) {
            ob_start();
        } else if (getenv('CFG_USE_AUTO_FLUSH')) {
            ob_implicit_flush(true);
        }

        if (getenv('CFG_EXTEND_MEMORY') == "true") {
            ini_set('memory_limit', getenv('CFG_EXTEND_MEMORY_TO'));
        }

        $this->output("Class Configured", true);
    }

    /************
    *
        syncS3
    *
    ************/
    public function syncS3()
    {
        $this->output("Starting S3 Sync...", true);

        $cmd = 'export AWS_DEFAULT_REGION="us-east-1";';
        $cmd .= getenv('AWS_BIN_PATH') . ' s3 sync ' . getenv('AWS_S3_SOURCE_URL') . ' ' . getenv('LOCAL_S3_TARGET_DIR') . ' 2>&1';

        $this->output(nl2br(shell_exec($cmd)), true);

        $this->done();
    }

    /************
    *
        syncS3
    *
    ************/
    public function processLocalS3Files()
    {
        $this->output("Processing Local S3 Files...", true);
        if ($this->getLastReadLocalS3File()) {
            $this->output("Starting from file " . ($this->getLastReadLocalS3File() +1). "...", true);
        }

        $local_s3_dir = getenv('LOCAL_S3_TARGET_DIR');

        # Scans the local S3 directory (potentially after sync) and creates an array with all the valid files
        if (is_dir($local_s3_dir) && ($dh = opendir($local_s3_dir))) {
            while (($file = readdir($dh)) !== false) {
                # Ignores hidden files
                if (strpos($file, ".") === 0) {
                    continue;
                }
                # Ignores files already read
                if ((int) $file <= $this->getLastReadLocalS3File()) {
                    continue;
                }
                $this->local_s3_files[] = $file;
            }
            natsort($this->local_s3_files);
            closedir($dh);

            # Read all files and creates an array containing the fields/json keys for further use (create database fields and insert rows in the events table)
            $total_rows_per_file = 0;
            foreach ($this->local_s3_files as $k => $file) {
                $line_count_helper = 0;
                if ($handle = fopen(getenv('LOCAL_S3_TARGET_DIR') . $file, 'r')) {
                    $this->output("$file ({$line_count_helper})");
                    while (!feof($handle)) {

                        $line = trim(fgets($handle));

                        if ($line) {
                            $total_rows_per_file++;
                            $this->total_local_s3_rows++;
                            if ((++$line_count_helper % 1000) == 0) {
                                $this->output("$file ({$line_count_helper})");
                            }
                            $json = $this->sanitizeDecodeJson($line);
                            if (!$json) {
                                $this->output(print_r("[{$file}] Couldn't parse json: " . htmlentities($line), true), true);
                            } else {
                                # Defines fields that are not in the json file but are in the database for processing/logic purposes
                                $json['_n_not_null'] = (empty($json['_n']) ? '' : $json['_n']);
                                $json['md5hash'] = '';

                                $array_keys = array_keys($json);

                                foreach ($array_keys as $n => $key) {
                                    $sanitized_key                        = $this->sanitizeKey($key);
                                    $val_length                           = strlen($json[$key]);
                                    $max_length                           = empty($this->sanitized_keys[$sanitized_key]) ? 0 : $this->sanitized_keys[$sanitized_key];
                                    $this->sanitized_keys[$sanitized_key] = ($val_length > $max_length) ? $val_length : $max_length;
                                    $this->original_keys[$key]            = $this->sanitized_keys[$sanitized_key];
                                }
                            }
                        }
                    }
                    $this->output("$file ({$line_count_helper})");
                    $this->output("", true);
                    fclose($handle);
                }
            }
        } else {
            $this->_die("Couldn't open or list the Local S3 Directory: {$local_s3_dir}");
        }

        $this->output("Total rows: {$this->total_local_s3_rows}", true);
        $this->done();

    }

    /************
    *
        createNewDatabaseFields
    *
    ************/
    public function createNewDatabaseFields()
    {
        $this->output("Creating New Database Fields...", true);

        $current_columns_qry = $this->databaseQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" . getenv('DB_TABLE') . "'");
        $current_columns_res = $this->databaseGetResult($current_columns_qry);
        $current_columns     = array();
        foreach ($current_columns_res as $r) {
            $current_columns[] = $r['column_name'];
        }

        foreach ($this->sanitized_keys as $key => $field_size) {
            # Ignores columns that already exists
            if (in_array($key, $current_columns)) {
                continue;
            }
            $this->output("New field {$key}...", true);
            if (!$this->databaseQuery("ALTER TABLE " . getenv('DB_TABLE') . " ADD " . $key . " " . getenv('DB_DEFAULT_FIELD_TYPE') . ";")) {
                $this->output("Field {$key}: " . $this->getDatabaseError(), true);
            }
            $this->databaseEmptyResult();
        }

        $this->done();
    }

    /************
    *
        insertNewEventsIntoDatabase
    *
    ************/
    public function insertNewEventsIntoDatabase()
    {
        $this->output("Inserting Data into the Database...", true);

        $last_output                          = 0;
        $curr_query_data                      = array();
        $insert_new_events_into_mysql_started = time();
        $eta                                  = '-';
        foreach ($this->local_s3_files as $file) {
            if ($handle = fopen(getenv('LOCAL_S3_TARGET_DIR') . $file, 'r')) {
                while (!feof($handle)) {
                    $line = trim(fgets($handle));

                    if ($line) {
                        $json = $this->sanitizeDecodeJson($line);
                        if ($json) {

                            $json['_n_not_null'] = (empty($json['_n']) ? '' : $json['_n']);

                            // Replaces empty with NULL for _n
                            if (isset($json['_n']) && $json['_n'] == '') {
                                unset($json['_n']);
                            }

                            // Formats _t from timestamp to YYYY-MM-DD HH:ii:ss
                            if (!empty($json['_t'])) {
                                $json['_t'] = date('Y-m-d H:i:s',$json['_t']);
                            }

                            $json['md5hash'] = md5(implode("",$json));

                            $fields        = array();
                            $insert_values = array();
                            foreach ($this->original_keys as $key => $field_size) {
                                if (!isset($json[$key]) || is_null($json[$key])) {
                                    $insert_values[] = "NULL";
                                } else {
                                    $insert_values[] = "'" . pg_escape_string($json[$key]) . "'";
                                }

                            }

                            $this->databaseStashInsertValues("(" . implode(",", $insert_values) . ")");

                            if (getenv('CFG_PROCESS_IDENTITIES') == "true") {
                                if (!empty($json['_p2'])) {
                                    $this->processIdentities($json['_p'],$json['_p2']);
                                }
                            }
                            
                            $total_queries_so_far = ($this->queriesDataIdentities + $this->queriesData);
                            if (($last_output + 1000) < $total_queries_so_far) {
                                $last_output = $total_queries_so_far;
                                $time_spent_so_far = time() - $insert_new_events_into_mysql_started;
                                if ($time_spent_so_far > 0 && $this->queriesData > 0) {
                                    $queries_per_second = $this->queriesData / $time_spent_so_far;
                                    $seconds_to_final   = ($this->total_local_s3_rows - $this->queriesData) / $queries_per_second;

                                    $eta = $this->secondsToTime(round($seconds_to_final));
                                }

                                $this->databaseOutput($eta,$file);
                            }
                        }
                    }
                }
                fclose($handle);
            }

            $this->databaseCommitStashValues();
            $this->databaseCommitIdentityStashValues();
            $this->databaseOutput($eta,$file);
            $insert_values = array();
            
            $this->setLastReadLocalS3File($file);
        }

        if (getenv('DB_OPTIMIZE_TABLES_AFTER_INSERTS') == "true") {
            $this->databaseOptimizeTables();
        }

        $this->output("", true);
        $this->done();

        if (getenv('DB_DELETE_DUPLICATES_AFTER_INSERTS') == "true") {
            $this->deleteDuplicates();
        }

    }

    /************
    *
        processIdentities
    *
    ************/
    private function processIdentities($identity1,$identity2)
    {
        $identities_to_be_added = array();

        $identities_to_be_added[$identity1] = $identity1;

        $identities_p = $this->getAliasesIdentity($identity1);

        foreach ($identities_p as $id) {
            $identities_to_be_added[$id] = $id;
        }

        $identities_to_be_added[$identity2] = $identity2;

        $identities_p2 = $this->getAliasesIdentity($identity2);

        foreach ($identities_p2 as $id) {
            $identities_to_be_added[$id] = $id;
        }

        foreach ($identities_to_be_added as $identity1) {
            foreach ($identities_to_be_added as $identity2) {

                if ($identity1 == $identity2)
                    continue;

                if ($this->isIdentityPairKnown($identity1,$identity2))
                    continue;

                $this->setKnownIdentity($identity1,$identity2);

                $this->databaseIdentityStashInsertValues("('{$identity1}','{$identity2}')");
            }
        }

    }

    /************
    *
        processIdentitiesFromAllDatapoints
    *
    ************/
    public function processIdentitiesFromAllDatapoints () {
        $this->output("Processing identities from the datapoints table...", true);

        $this->registerAllKnownIdentities();

        $this->output("Reading aliases...", true);
        $aliases_read = 0;
        $aliases = $this->getAllAliasesDatapoints();
        $identities_to_add = array();
        foreach ($aliases as $alias) {
            $identity1 = $alias['_p'];
            $identity2 = $alias['_p2'];

            // If has @ symbol (email), ignores it
            if (strpos($identity1, '@') !== FALSE)
                continue;

            $this->manageIdentityPairs($identity1,$identity2);
            if (count($this->known_identity_pairs) > ($aliases_read+1000)) {
                $aliases_read = count($this->known_identity_pairs);
                $this->output("({$aliases_read})");
            }
        }

        $this->output("Reading emails updates...", true);
        $datapoints = $this->getAllUpdatedEmailDatapoints();
        foreach ($datapoints as $datapoint) {
            $identity1 = $datapoint['_p'];
            $identity2 = $datapoint['new_email'];

            $this->manageIdentityPairs($identity1,$identity2);

            $identity1 = $datapoint['new_email'];
            $identity2 = $datapoint['previous_email'];

            $this->manageIdentityPairs($identity1,$identity2);

            if (count($this->known_identity_pairs) > ($aliases_read+1000)) {
                $aliases_read = count($this->known_identity_pairs);
                $this->output("({$aliases_read})");
            }
        }

        $this->output("Saving identities to the database...", true);
        foreach ($this->known_identity_pairs as $id1 => $identities) {
            foreach ($identities as $id2 => $identity) {
                $this->databaseIdentityStashInsertValues("('{$id1}','{$id2}')");
            }
        }

        // Saves new identities to the database
        $this->databaseCommitIdentityStashValues();

        $this->output("Done!", true);
    }

    /************
    *
        isIdentityPairKnown
    *
    ************/
    private function isIdentityPairKnown($identity1,$identity2) {
        // To save memory
        $concat_identities = $this->concatIdentityPair($identity1,$identity2);

        // If identity pair is previously known, returns true
        if (!empty($this->known_identities[$concat_identities])) {
            return true;
        }

        return false;
    }

    /************
    *
        concatIdentityPair
    *
    ************/
    private function concatIdentityPair($identity1,$identity2) {
        $concat_identities = $identity1 . '||' . $identity2;
        if (strlen($concat_identities) > 32) {
            $concat_identities = md5($concat_identities);
        }

        return $concat_identities;
    }

    /************
    *
        setKnownIdentity
    *
    ************/
    private function setKnownIdentity($id1, $id2) {
        $concat_ids = $this->concatIdentityPair($id1,$id2);
        $this->known_identities[$concat_ids] = true;
        $this->manageIdentityPairs($id1,$id2);
    }

    /************
    *
        manageIdentityPairs
    *
    ************/
    private $identitiesFile;

    private function manageIdentityPairs($id1, $id2) {
        # hold all identities from this user here
        $identities = [$id1, $id2];

        # Verify if the current identity already exists
        # into out has table and remove it
        foreach ([$id1, $id2] as $id) {
            $hash = sha1($id);
            if (!array_key_exists($hash, $this->identities)) {
                continue;  # the identity is new, let's skip it
            }
            $identities = array_merge($identities, $this->identities[$hash]);
        }

        # Remove duplicate identities
        $identities = array_unique($identities);

        # Order the identities alphabeticly ascending
        sort($identities, SORT_NATURAL | SORT_FLAG_CASE);

        $sign = sha1(implode('|', $identities));

        if (

        foreach ([$id1, $id2] as $id) {
            if (!array_key_exists($id, $this->known_identity_pairs)) {
                continue;  # go ahead since it's not exists
            }

            $identities_to_change = array_unique(
                array_merge(
                    $this->known_identity_pairs[sha1($id)],
                    $identities_to_change
                )
            );

            $


        if (!empty($this->known_identity_pairs[$id1])) {
            $identities_to_change = array_unique(array_merge($this->known_identity_pairs[$id1],$identities_to_change));
        }

        if (!empty($this->known_identity_pairs[$id2])) {
            $identities_to_change = array_unique(array_merge($this->known_identity_pairs[$id2],$identities_to_change));
        }

        foreach($identities_to_change as $v1) {
            foreach($identities_to_change as $v2) {
                $this->known_identity_pairs[$v1][$v2] = $v2;
                $this->known_identity_pairs[$v2][$v1] = $v1;
            }
        }
    }

    /************
    *
        getAllAliasesDatapoints
    *
    ************/
    private function getAllAliasesDatapoints() {
        $query    = "SELECT * FROM " . getenv('DB_TABLE'). " WHERE _p2 != '';";

        $alias_qry     = $this->databaseQuery($query);
        $alias_res     = $this->databaseGetResult($alias_qry);
        $aliases = array();
        foreach ($alias_res as $row) {
            $aliases[] = array(
                '_p' => $row['_p'],
                '_p2' => $row['_p2']
            );
        }
        return $aliases;
    }

    /************
    *
        getAllUpdatedEmailDatapoints
    *
    ************/
    private function getAllUpdatedEmailDatapoints() {
        $query    = "SELECT * FROM " . getenv('DB_TABLE'). " WHERE _n_not_null LIKE 'updated email';";

        $updated_email_qry     = $this->databaseQuery($query);
        $updated_email_res     = $this->databaseGetResult($updated_email_qry);
        $datapoints = array();
        foreach ($updated_email_res as $row) {
            $datapoints[] = array(
                '_p' => $row['_p'],
                'new_email' => $row['new_email'],
                'previous_email' => $row['previous_email']
            );
        }
        return $datapoints;
    }

    /************
    *
        registerAllKnownIdentities
    *
    ************/
    private function registerAllKnownIdentities() {
        $query    = "SELECT * FROM " . getenv('DB_IDENTITIES_TABLE');

        $id_qry     = $this->databaseQuery($query);
        $id_res     = $this->databaseGetResult($id_qry);
        foreach ($id_res as $id) {
            $this->manageIdentityPairs($id['identity1'],$id['identity2']);
        }
        return true;
    }

    /************
    *
        getAliasesIdentity
    *
    ************/
    private function getAliasesIdentity($identity)
    {
        $identity = pg_escape_string($identity);
        $query    = "SELECT identity1 FROM " . getenv('DB_IDENTITIES_TABLE') . " WHERE identity2 = '{$identity}'";

        $id_qry     = $this->databaseQuery($query);
        $id_res     = $this->databaseGetResult($id_qry);
        $identities = array();
        foreach ($id_res as $id) {
            $identities[] = $id['identity1'];
        }
        return $identities;
    }

    /************
    *
        databaseOptimizeTables
    *
    ************/
    private function databaseOptimizeTables()
    {
        $this->output("Optimizing Tables", true);
        $this->databaseQuery("VACUUM FULL " . getenv('DB_TABLE'));

        if (getenv('DB_IDENTITIES_TABLE'))
            $this->databaseQuery("VACUUM FULL " . getenv('DB_IDENTITIES_TABLE'));
    }

    /************
    *
        deleteDuplicates
    *
    ************/
    private function deleteDuplicates()
    {
        $this->output("Deleting Duplicates... (@TBD)", true);
        $this->done();
        return;

        $base_fields = array();
        foreach ($this->sanitized_keys as $key => $field_size) {
            $base_fields[] = 't1.`' . $key . '`=t2.`' . $key . '`';
        }

        $query = "delete t1 from " . getenv('DB_TABLE') . " t1, " . getenv('DB_TABLE') . " t2 WHERE " . implode(' AND ', $base_fields);

        $this->databaseQuery($query);

        $this->done();
    }

    /************
    *
        getBaseInsertDatabaseQuery
    *
    ************/
    public function getBaseInsertDatabaseQuery()
    {

        if (empty($this->base_insert_qry)) {
            $base_fields = array();
            foreach ($this->sanitized_keys as $key => $field_size) {
                $base_fields[] = $key;
            }
            $this->base_insert_qry = "INSERT INTO " . getenv('DB_TABLE') . " (" . implode(',', $base_fields) . ") VALUES ";
        }

        return $this->base_insert_qry;

    }

    /************
    *
        getBaseIdentityDatabaseQuery
    *
    ************/
    public function getBaseIdentityDatabaseQuery()
    {
        if (empty($this->base_identity_insert_qry)) {
            $this->base_identity_insert_qry = "INSERT INTO " . getenv('DB_IDENTITIES_TABLE') . " (identity1, identity2) VALUES ";
        }

        return $this->base_identity_insert_qry;

    }

    /************
    *
        getLastReadLocalS3File
    *
    ************/
    private function getLastReadLocalS3File()
    {
        if ($this->last_read_local_s3_file === null) {
            if (!is_file(getenv('LOCAL_S3_LAST_READ_FILENAME'))) {
                $this->setLastReadLocalS3File("");
            }
            $this->last_read_local_s3_file = file_get_contents(getenv('LOCAL_S3_LAST_READ_FILENAME'));
        }

        # Returns the number of the filename read (eg. for "1234.json", returns "1234")
        return (int) $this->last_read_local_s3_file;
    }

    /************
    *
        setLastReadLocalS3File
    *
    ************/
    private function setLastReadLocalS3File($file)
    {
        file_put_contents(getenv('LOCAL_S3_LAST_READ_FILENAME'), $file);
    }

    /************
    *
        rollbackLastReadLocalS3File
    *
    ************/
    private function rollbackLastReadLocalS3File()
    {
        $this->setLastReadLocalS3File($this->starting_read_local_s3_file);
    }

    /************
    *
        databaseConnect
    *
    ************/
    public function databaseConnect()
    {
        $this->output("Connecting to Database...", true);
        try {
            $this->db_conn = pg_connect("host=".getenv('DB_URL')." port=".getenv('DB_PORT')." dbname=".getenv('DB_DBNAME')." user=".getenv('DB_USER')." password=".getenv('DB_PASSWORD'));
        } catch (Exception $e) {
            $this->rollback_last_read_local_s3_file();
            $this->_die($e->getMessage());
        }

        $this->done();
    }

    /************
    *
        databaseQuery
    *
    ************/
    private function databaseQuery($query)
    {
        if (empty($this->db_conn)) {
            $this->databaseConnect();
        }

        if (($qry = pg_query($this->db_conn, $query)) === false)
            $this->_die(print_r($query, true) . "<br>" . $this->db_conn->error);

        return $qry;
    }

    /************
    *
        databaseEmptyResult
    *
    ************/
    private function databaseEmptyResult () {
        while (pg_get_result($this->db_conn)) {}
    }

    /************
    *
        databaseGetResult
    *
    ************/
    private function databaseGetResult($result)
    {
        $r = array();
        while ($a = pg_fetch_assoc($result)) {
            $r[] = $a;
        }
        return $r;
    }

    /************
    *
        databaseStashInsertValues
    *
    ************/
    private function databaseStashInsertValues($query)
    {
        $this->db_stash_insert_values[] = $query;

        if (count($this->db_stash_insert_values) >= getenv('DB_QUERIES_PER_CALL')) {
            $this->databaseCommitStashValues();
        }
    }

    /************
    *
        databaseIdentityStashInsertValues
    *
    ************/
    private function databaseIdentityStashInsertValues($query)
    {
        $this->db_identity_stash_insert_values[] = $query;

        if (count($this->db_identity_stash_insert_values) >= getenv('DB_QUERIES_PER_IDENTITY_CALL')) {
            $this->databaseCommitIdentityStashValues();
        }
    }

    /************
    *
        databaseCommitStashValues
    *
    ************/
    private function databaseCommitStashValues()
    {
        if (!empty($this->db_stash_insert_values)) {
            while (count($this->db_stash_insert_values)) {
                $splice = array_splice($this->db_stash_insert_values, 0, getenv('DB_QUERIES_PER_CALL'));
                $query = $this->getBaseInsertDatabaseQuery() . implode(",", $splice);
                $this->databaseQuery($query);
                $this->queriesData += count($splice);
                $this->databaseEmptyResult();
            }
        }
    }

    /************
    *
        databaseCommitIdentityStashValues
    *
    ************/
    private function databaseCommitIdentityStashValues()
    {
        if (!empty($this->db_identity_stash_insert_values)) {
            while (count($this->db_identity_stash_insert_values)) {
                $splice = array_splice($this->db_identity_stash_insert_values, 0, getenv('DB_QUERIES_PER_IDENTITY_CALL'));
                $query = $this->getBaseIdentityDatabaseQuery() . implode(",", $splice);
                $this->databaseQuery($query);
                $this->queriesDataIdentities += count($splice);
                $this->databaseEmptyResult();
            }
        }
    }

    /************
    *
        getDatabaseError
    *
    ************/
    private function getDatabaseError()
    {
        return pg_last_error($this->db_conn);
    }

    /************
    *
        done
    *
    ************/
    private function done()
    {
        $this->output("&#x2713; Done<hr />", true);
    }

    /************
    *
        createJsBaseScript
    *
    ************/
    private function createJsBaseScript()
    {
        echo "	<script>
					function m(id, msg) {
						var tp = document.getElementById(id);
						if (tp) {
							tp.parentNode.removeChild(document.getElementById(id));
						}
						var dv = document.createElement('div');
						dv.innerHTML = msg;
						dv.setAttribute('id', id);
						document.body.appendChild(dv);
						window.scrollTo(0,document.body.scrollHeight);
					}
				</script>";
    }

    /************
    *
        databaseOutput
    *
    ************/
    public function databaseOutput($eta,$file) {
        $qtyIdentities = count($this->known_identities);
        $this->output("ETA: {$eta}
                        <br>Current File: {$file}
                        <br>Datapoints: {$this->queriesData} (" . round((($this->queriesData) / $this->total_local_s3_rows) * 100, 1) . "%)
                        <br>Identitiy Pairs Found: {$qtyIdentities}");
    }
    /************
    *
        output
    *
    ************/
    public function output($msg, $new_group = false)
    {

        if ($new_group) {
            $this->output_group++;
        }

        if (getenv('CFG_USE_JAVASCRIPT_OUTPUT') == "true" && !$this->isPhpCli()) {
            $this->outputJs($msg);
        } else {
            $this->outputNatural($msg);
        }

        if ($new_group) {
            $this->output_group++;
        }

        if (getenv('CFG_USE_AUTO_FLUSH') == "true" && !(getenv('CFG_EMAIL_SEND_OUTPUT_EMAIL_WHEN_PHP_CLI') && $this->isPhpCli())) {
            flush();
            ob_flush();
        }
    }

    /************
    *
        outputJs
    *
    ************/
    public function outputJs($msg)
    {
        $msg = str_replace(array("\r", "\n"), "", $msg);
        $msg = str_replace("'", "&#39;", $msg);
        echo PHP_EOL . "<script>m('{$this->output_group}', '$msg');</script>";
    }

    /************
    *
        outputNatural
    *
    ************/
    public function outputNatural($msg)
    {
        echo PHP_EOL . $msg . "<br />";
    }

    /************
    *
        sanitizeDecodeJson
    *
    ************/
    private function sanitizeDecodeJson($string)
    {
    /************
    *
        "/(.*?
    *
    ************/
        $string = preg_replace_callback("/(.*?)(\ ?\:\ {0,}?\"?)(.*?)(\"?(}|, |,))/", function ($m) {
            return $m[1] . $m[2] . addslashes($m[3]) . $m[4];
        }, $string);
        $string = str_replace('\\', '\\\\', $string);
        $string = str_replace('\\\\"', '\\"', $string);
        return json_decode($string, true);
    }

    /************
    *
        sanitizeKey
    *
    ************/
    private function sanitizeKey($key)
    {
        return str_replace(array('-', '_', ' '), '_', $key);
    }

    /************
    *
        _die
    *
    ************/
    public function _die($msg)
    {
        $this->dead = true;
        $this->output($msg, true);

        if (getenv('CFG_EMAIL_SEND_OUTPUT_EMAIL_WHEN_PHP_CLI') == "true" && $this->isPhpCli()) {
            $output = ob_get_clean();
            $this->sendMail($output);
        }

        exit;
    }

    /************
    *
        sendMail
    *
    ************/
    public function sendMail($content)
    {
        $mail = new \PHPMailer;

        $mail->isSMTP();
        $mail->Host       = getenv('CFG_EMAIL_HOST');
        $mail->SMTPAuth   = true;
        $mail->Username   = getenv('CFG_EMAIL_USERNAME');
        $mail->Password   = getenv('CFG_EMAIL_PASSWORD');
        $mail->SMTPSecure = 'tls';
        $mail->Port       = getenv('CFG_EMAIL_PORT');

        $mail->setFrom(getenv('CFG_EMAIL_FROM'));
        $mail->addAddress(getenv('CFG_EMAIL_TO'));

        $mail->isHTML(true);

        $mail->Subject = getenv('CFG_EMAIL_SUBJECT');
        $mail->Body    = $content;

        if (!$mail->send()) {
            echo 'Message could not be sent.';
            echo 'Mailer Error: ' . $mail->ErrorInfo;
        } else {
            echo 'Message has been sent';
        }
    }

    /************
    *
        isPhpCli
    *
    ************/
    public function isPhpCli()
    {
        return (php_sapi_name() === 'cli');
    }

    /**
     * Get human readable time difference between 2 dates
     *
     * Return difference between 2 dates in year, month, hour, minute or second
     * The $precision caps the number of time units used: for instance if
     * $time1 - $time2 = 3 days, 4 hours, 12 minutes, 5 seconds
     * - with precision = 1 : 3 days
     * - with precision = 2 : 3 days, 4 hours
     * - with precision = 3 : 3 days, 4 hours, 12 minutes
     *
     * From: http://www.if-not-true-then-false.com/2010/php-calculate-real-differences-between-two-dates-or-timestamps/
     *
     * @param mixed $time1 a time (string or timestamp)
     * @param mixed $time2 a time (string or timestamp)
     * @param integer $precision Optional precision
     * @return string time difference
     */
    /************
    *
        getDateDiff
    *
    ************/
    public function getDateDiff($time1, $time2, $precision = 3)
    {
        // If not numeric then convert timestamps
        if (!is_int($time1)) {
            $time1 = strtotime($time1);
        }
        if (!is_int($time2)) {
            $time2 = strtotime($time2);
        }
        // If time1 > time2 then swap the 2 values
        if ($time1 > $time2) {
            list($time1, $time2) = array($time2, $time1);
        }
        // Set up intervals and diffs arrays
        $intervals = array('year', 'month', 'day', 'hour', 'minute', 'second');
        $diffs     = array();
        foreach ($intervals as $interval) {
            // Create temp time from time1 and interval
            $ttime = strtotime('+1 ' . $interval, $time1);
            // Set initial values
            $add    = 1;
            $looped = 0;
            // Loop until temp time is smaller than time2
            while ($time2 >= $ttime) {
                // Create new temp time from time1 and interval
                $add++;
                $ttime = strtotime("+" . $add . " " . $interval, $time1);
                $looped++;
            }
            $time1            = strtotime("+" . $looped . " " . $interval, $time1);
            $diffs[$interval] = $looped;
        }
        $count = 0;
        $times = array();
        foreach ($diffs as $interval => $value) {
            // Break if we have needed precission
            if ($count >= $precision) {
                break;
            }
            // Add value and interval if value is bigger than 0
            if ($value > 0) {
                if ($value != 1) {
                    $interval .= "s";
                }
                // Add value and interval to times array
                $times[] = $value . " " . $interval;
                $count++;
            }
        }
        // Return string with times
        return implode(", ", $times);
    }

    /************
    *
        secondsToTime
    *
    ************/
    private function secondsToTime($seconds)
    {
        $dtF = new \DateTime('@0');
        $dtT = new \DateTime("@$seconds");
        $h   = str_pad($dtF->diff($dtT)->format('%h'), 2, "0", STR_PAD_LEFT);
        $i   = str_pad($dtF->diff($dtT)->format('%i'), 2, "0", STR_PAD_LEFT);
        $s   = str_pad($dtF->diff($dtT)->format('%s'), 2, "0", STR_PAD_LEFT);

        return "{$h}:{$i}:{$s}";
    }

}
