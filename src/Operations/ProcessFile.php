<?php

namespace KissmetricsToDatabase\Operations;

use SplFileInfo;
use SplFileObject;

class ProcessFile
{
    /**
     * @var SplFileInfo $file
     */
    private $file;

    /**
     * @param SplFileInfo $file
     * @return $this
     */
    public function __construct(SplFileInfo $file)
    {
        $this->file = $file;
    }

    public function row()
    {
        $f = $this->file->openfile();
        while (!$f->eof()) {
            $line = trim($f->fgets());
            if (empty($line)) {
                continue;
            }

            $data = json_decode_sanitize($line);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new \RuntimeException(json_last_error_msg());
            }

            // Defines fields that are not in the json file
            // but are in the database for processing/logic purposes
            $data['_n_not_null'] = (empty($data['_n']) ? '' : $data['_n']);
            $data['md5hash'] = '';

            // return the current data to be processed
            yield array_key_sanitize($data);
        }
    }
}

