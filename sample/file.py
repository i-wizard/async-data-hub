from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

def expensive_op(value: int):
    pass

items = [1, 2, 3, 4, 5]

with ProcessPoolExecutor() as executor:
    results = list(executor.map(expensive_op, items))

with ThreadPoolExecutor as executor:
    t_results = list(executor.map(expensive_op, items))



from functools import wraps

def logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Calling {func.__name__}")
        result = func(*args, **kwargs)
        print(f"Finished {func.__name__}")
        return result

    return wrapper


@logger
def add(a, b):
    return a + b


print(add(2, 3))



def retry(max_retries):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if attempt == max_retries - 1:
                        raise

        return wrapper

    return decorator


@retry(max_retries=3)
def fetch_data():
    print("Fetching data...")
    raise Exception("Failed")


fetch_data()


class DatabaseConnection:
    def __enter__(self):
        print("Opening connection")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("Closing connection")


with DatabaseConnection() as db:
    print("Doing database work")



from contextlib import contextmanager

@contextmanager
def database_connection():
    print("Opening connection")

    try:
        yield
    finally:
        print("Closing connection")


with database_connection():
    print("Doing database work")