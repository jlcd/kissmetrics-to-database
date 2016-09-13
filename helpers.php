<?php
/**
 * Collection of useful functions
 */

/**
 * @param string $str
 * @return mixed
 */
function json_decode_sanitize($str)
{
    $str = preg_replace_callback(
        "/(.*?)(\ ?\:\ {0,}?\"?)(.*?)(\"?(}|, |,))/",
        function ($m) {
            return $m[1] . $m[2] . addslashes($m[3]) . $m[4];
        },
        $str
    );
    $str = str_replace('\\', '\\\\', $str);
    $str = str_replace('\\\\"', '\\"', $str);
    return json_decode($str, true);
}

/**
 * @param array $arr
 * @return array
 */
function array_key_sanitize(array $arr)
{
    $keys = array_map(function ($k) {
        return str_replace(['-', '_', ' '], '_', $k);
    }, array_keys($arr));

    return array_combine($keys, array_values($arr));
}

