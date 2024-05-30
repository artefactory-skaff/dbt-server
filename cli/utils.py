def rename(new_name):
    def decorator(f):
        f.__name__ = new_name
        return f
    return decorator
