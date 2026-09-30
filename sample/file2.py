import argparse
import subprocess
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from contextvars import ContextVar
from multiprocessing.pool import ThreadPool
from time import sleep
from typing import Callable

from functools import lru_cache

counter = 0
lock = threading.Lock()


def increment_thread_safe() -> None:
    global counter
    for _ in range(10):
        with lock:
            current = counter
            sleep(0)
            counter = current + 1


def increment_naive():
    global counter
    for _ in range(10):
        current = counter
        sleep(0)
        counter = current + 1


def with_thread():
    print(counter)
    threads = []
    for _ in range(3):
        thread = threading.Thread(target=increment_naive())
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    print(counter)


def with_thread_safe():
    print(counter)
    threads = []
    for i in range(3):
        thread = threading.Thread(target=increment_thread_safe(), name=f"Thread-{i}")
        threads.append(thread)
        thread.start()

    print("joining threads")
    for thread in threads:
        print(f"Joining {thread.name}")
        thread.join()
    print(counter)


class BankAccount:
    def __init__(self):
        self.balance = 0
        self.lock = threading.RLock()

    def deposit(self, amount):
        # bad
        # current_balance = self.balance
        # sleep(0)
        # self.balance = current_balance + amount
        # self._log_transaction()

        # Good
        with self.lock:
            self.balance += amount
            self._log_transaction()

    def _log_transaction(self):
        with self.lock:
            print("Transaction logged")


def bulk_deposit(account: Callable[[], None], amount, times):
    # with ThreadPool(processes=5) as pool:
    #     pool.starmap(account.deposit, [(amount,)] * times)
    # with ThreadPoolExecutor() as executor:
    #     list(executor.map(account.deposit, [amount] * times))
    threads = []
    for _ in range(times):
        thread = threading.Thread(target=account.deposit, args=(amount,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


class ThreadJoinSample:
    @staticmethod
    def _test_writing_while_process_exits():
        print("write file after 3 seconds")
        sleep(3)
        with open("test.txt", "w") as f:
            f.write("Hello, world!")
        print("File written.")

    @staticmethod
    def delayed_task():
        print("Starting delayed task...")
        print(f"Executed by thread: {threading.current_thread().name}")
        sleep(1)
        print("Delayed task completed.")
        # how do i print out the name of the thread executing this function?

    @classmethod
    def _test_thread_join(cls):
        print(
            f"{threading.current_thread().name} running the _test_thread_join function"
        )
        thread = threading.Thread(target=cls.delayed_task, name="DelayedTaskThread")
        write_file_thread = threading.Thread(
            target=cls._test_writing_while_process_exits,
            name="WriteFileThread",
            daemon=True,
        )
        thread.start()
        print(f"{threading.current_thread().name} could be doing other things")
        thread.join(timeout=3)  # Wait for the thread to finish for max time at timeout
        write_file_thread.start()
        print(f"{threading.current_thread().name} continues after delayed task.")


class ContextVarSample:
    my_var: ContextVar[bool] = ContextVar("my_var", default=False)

    def fun1(self):
        token = self.my_var.set(True)
        print(self.my_var.get())

    def fun2(self):
        print(self.my_var.get())
        token = self.my_var.set(False)
        print(self.my_var.get())
        self.my_var.reset(token)
        print(self.my_var.get())


def run_script():
    result = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], text=True
    ).strip()
    print("res", result)


def args_check():
    parser = argparse.ArgumentParser(
        description="Run a script with optional arguments.", prog="MyScript"
    )
    parser.add_argument("--name", type=str, help="Your name")
    parser.add_argument("--print", action="store_true")
    parser.add_argument("--check", action="store_false", dest="check2")
    parser.add_argument("--check2", action="store_true")
    parser.add_argument("--tags", action="append")
    # parser.add_argument("color", type=str)
    subparser = parser.add_subparsers(dest="subcmds")
    color_parser = subparser.add_parser("color")
    color_parser.add_argument("--shade")

    shape_parser = subparser.add_parser("shape")
    shape_parser.add_argument("--size", type=int)
    # parser.add_argument("black", type=str, dest='color')
    print("args", parser.parse_args())

@lru_cache(maxsize=10)
def saved_num():
    print("saved_num ran")
    return 1


def check_saved_num():
    count = 5
    for _ in range(5):
        num = saved_num()
        print(num)
    return


if __name__ == "__main__":
    # account = BankAccount()
    # bulk_deposit(account, 100, 10)
    # print(f"Final balance: {account.balance}")
    # assert account.balance == 1000, f"Expected balance to be 1000, but got {account.balance}"
    # ThreadJoinSample._test_thread_join()

    # ContextVarSample().fun1()
    # ContextVarSample().fun2()
    check_saved_num()
