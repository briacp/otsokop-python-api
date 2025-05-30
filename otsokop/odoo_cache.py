import functools
import logging


def odoo_cache(cache_key=None, ttl=None, force_fetch=False):
    """
    Decorator to cache method results.
    - cache_key: Optional key (can be a string or a callable that receives *args, **kwargs).
    - ttl: Time-to-live in seconds (optional).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _args = list(args)
            _self = _args.pop(0)

            # If cache_key is a callable, call it with args/kwargs; else, auto-generate
            if callable(cache_key):
                key = _self.cache_key(*_args, **kwargs)
            elif cache_key is not None:
                key = cache_key
            else:
                # Default: use function name and arguments
                args_repr = "_".join(map(str, _args))
                kwargs_repr = "_".join(f"{k}={v}" for k, v in kwargs.items())
                key = f"{func.__name__}:{args_repr}:{kwargs_repr}"

            # Check cache
            if hasattr(_self, "_check_cache"):
                logging.debug(f"odoo_cache/check_cache({key})")
                cached = _self._check_cache(key)
                if not force_fetch and cached is not None:
                    return cached

            # Compute and cache value
            result = func(*args, **kwargs)
            if hasattr(_self, "_set_cache"):
                logging.debug(f"odoo_cache/set_cache({key}, {ttl})")
                _self._set_cache(key, result, expire=ttl)
            return result

        return wrapper

    return decorator
