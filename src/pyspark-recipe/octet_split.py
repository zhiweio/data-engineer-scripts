from typing import List

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType


def decode_utf8(data: bytes) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def octet_split(data: bytes, size: int) -> List[str]:
    """Split a byte array into a list of valid UTF-8 string, adjusting byte size as needed.

    This function splits a byte string into strings based on the specified
    byte size, dynamically adjusting the size to ensure valid UTF-8 decoding.

    :param data: The byte array to be split.
    :param size: The approximate maximum byte size for splitting string.
    :return: A list of valid UTF-8 decoded strings.
    :raises ValueError: If size is less than or equal to 0,
                       or if no valid UTF-8 strings can be formed.
    """
    if size <= 0:
        raise ValueError("byte_size must be greater than 0")

    strings = []
    start = 0
    length = len(data)

    while start < length:
        end = start + size
        if end > length:
            end = length

        chunk = data[start:end]
        string = decode_utf8(chunk)

        if string:
            strings.append(string)
            start += len(chunk)
            size = max(size, len(chunk))
        else:
            while size > 0:
                size -= 1
                end = start + size
                if end <= start:
                    break
                chunk = data[start:end]
                string = decode_utf8(chunk)

                if string:
                    strings.append(string)
                    start += len(chunk)
                    break
            else:
                raise ValueError("Unable to split bytes into valid UTF-8 strings")

    return strings


@F.udf(returnType=ArrayType(StringType()))
def octet_split_udf(text: str, size: int = 65535):
    """User-defined function to split text into valid UTF-8 strings.

    :param text: The input text string to be split.
    :param size: The approximate maximum byte size for splitting string. Default is 65535 bytes.
    :return: A list of valid UTF-8 decoded strings.
    """
    if text is None:
        return

    data = text.encode("utf8")
    return octet_split(data, size)


@F.udf(returnType=ArrayType(StringType()))
def octet_pad_split_udf(text: str, width: int, size: int = 65535):
    """User-defined function to split text into fixed columns of valid UTF-8 strings.

    :param text: The input text string to be split.
    :param width: The width of the columns array should be filled with splitting strings
                  and padded with empty strings.
    :param size: The approximate maximum byte size for splitting string. Default is 65535 bytes.
    :return: A list of valid UTF-8 strings. If there are fewer strings than columns,
             empty strings are returned for the remaining columns.
    """
    if text is None:
        return

    data = text.encode("utf-8")
    strings = octet_split(data, size)
    strings = strings + [""] * (width - len(strings))
    return strings[:width]  # Ensure the result is exactly the length of width
