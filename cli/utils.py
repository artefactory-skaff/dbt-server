def rename(new_name):
    """
    A decorator to rename a function.

    This decorator changes the __name__ attribute of the function to the specified new_name.
    It is useful for dynamically renaming functions, especially when creating commands or
    dynamically generated functions.

    Args:
        new_name (str): The new name to assign to the function.

    Returns:
        function: The decorated function with the new name.
    """
    def decorator(f):
        f.__name__ = new_name
        return f
    return decorator
