<?php

namespace KissmetricsToDatabase\Operations;

use SplFileInfo;
use SplFileObject;

class ProcessFile implements OperationInterface
{
    /**
     * @var SplFileInfo $file
     */
    private $file;

    /**
     * @param SplFileInfo $file
     * @return $this
     */
    public function executeWithFile(SplFileInfo $file)
    {
        $this->file = $file;
        
        $this->execute();

        return $this;
    }

    public function execute()
    {
        $f = $this->file->openfile();
        $f->setFlags(SplFileObject::DROP_NEW_LINE | SplFileObject::SKIP_EMPTY);
        while (!$f->eof()) {
            $line = $f->fgets();
            $data = json_decode($line, true, 512, JSON_UNESCAPED_UNICODE);
            if (json_last_error() !== JSON_ERROR_NONE) {
                var_dump(mb_list_encodings());
                echo $line . PHP_EOL;
                echo mb_convert_encoding($line, "UTF-8", "ISO-8859-1") . PHP_EOL;
                echo htmlentities($line, ENT_QUOTES | ENT_IGNORE, "UTF-8");
                throw new \RuntimeException(json_last_error_msg());
            }

            var_dump($data);
        }
    }
}

