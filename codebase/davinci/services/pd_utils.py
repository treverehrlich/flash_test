import numpy as np
import functools

def or_list_mask(*conditions):
    """
    Utility function to build a mask out of
    a list of many intended "or" statements.
    This can be very handy if you can programmatically
    define the masks you want to include.

    :return: numpy mask

    Example usage:

        .. code-block:: python

            from davinci.services.pd import or_list

            # instead of doing this:
            df_filtered = df[
                (df['a'] == 1) | (df['b'] == 2) | df['c'] == 3)
            ]

            # you can do this:
            pairs = list(zip('abc', range(1, 4)))
            conditions = [df[a] == b for a, b in pairs]
            df_filtered = df[or_list_mask(*conditions)]

    """
    return functools.reduce(np.logical_or, conditions)

def and_list_mask(*conditions):
    """
    Utility function to build a mask out of
    a list of many intended "and" statements.
    This can be very handy if you can programmatically
    define the masks you want to include.

    :return: numpy mask

    Example usage:

        .. code-block:: python

            from davinci.services.pd import or_list

            # instead of doing this:
            df_filtered = df[
                (df['a'] == 1) & (df['b'] == 2) & df['c'] == 3)
            ]

            # you can do this:
            pairs = list(zip('abc', range(1, 4)))
            conditions = [df[a] == b for a, b in pairs]
            df_filtered = df[and_list_mask(*conditions)]

    """
    return functools.reduce(np.logical_and, conditions)