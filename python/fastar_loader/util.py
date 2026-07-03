import numpy as np


def translate_integers(
    array: np.ndarray, mapping: dict[int, int], dtype: np.typing.DTypeLike | None = None
) -> np.ndarray:
    """Translates all integers in a tensor by a mapping.

    Args:
        array: A tensor of integers.
        mapping: A dictionary mapping integers to their new values.
        dtype: The dtype of the output tensor. If `None`, the dtype of `array` is used.

    Returns:
        A tensor with the same shape as `array` where each integer has been replaced by its mapped value.
    """
    if dtype is None:
        dtype = array.dtype

    # https://stackoverflow.com/a/16993364
    unique, inverse = np.unique(array, return_inverse=True)
    mapped_unique = np.array([mapping[i] for i in unique], dtype=dtype)
    return mapped_unique[inverse].reshape(array.shape)


def ascii_to_sequence(ascii: np.ndarray | bytes, reverse_complement: bool = False) -> np.ndarray:
    if isinstance(ascii, bytes):
        ascii = np.frombuffer(ascii, dtype=np.uint8)
    other = {ord(nt): 4 for nt in "WSMKRYBDHVNwsmkrybdhvn"}
    if not reverse_complement:
        return translate_integers(
            ascii,
            {
                ord("A"): 0,
                ord("a"): 0,
                ord("C"): 1,
                ord("c"): 1,
                ord("G"): 2,
                ord("g"): 2,
                ord("T"): 3,
                ord("t"): 3,
                **other,
            },
            dtype=np.int64,
        )
    else:
        return translate_integers(
            np.flip(ascii, (-1,)),
            {
                ord("A"): 3,
                ord("a"): 3,
                ord("C"): 2,
                ord("c"): 2,
                ord("G"): 1,
                ord("g"): 1,
                ord("T"): 0,
                ord("t"): 0,
                **other,
            },
            dtype=np.int64,
        )


def ascii_to_softmask(ascii: np.ndarray | bytes) -> np.ndarray:
    if isinstance(ascii, bytes):
        ascii = np.frombuffer(ascii, dtype=np.uint8)
    # softmask is indicated by lower-case letters, which are in a continuous range in ASCII
    return np.logical_and(ascii >= ord("a"), ascii <= ord("z"))
