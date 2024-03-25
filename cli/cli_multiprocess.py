## Standard Libraries
import os
import sys
import time
import math
import random
from functools import cache, partial
from typing import Callable
from types import CodeType, LambdaType, FunctionType
import json

## File Management
from pathlib import Path
import pickle
import copyreg

## Multi-processing
import signal
from multiprocessing import Process, Queue, Pipe, cpu_count
from multiprocessing.connection import Connection
import psutil
import atexit


### Purpose: A more memory efficient way of managing worker processes throughout the program
# The problem with the multiprocessing library is that when a worker process is created,
# it copies the memory of the parent process. This is not a problem for small programs,
# but when working with large datasets, this can quickly lead to memory issues.
# More specifically, when we attempt to subset and parallelize the geometry of the full US,
# we quickly run out of memory.
# We can sidestep this issue by creating the set of worker processes in an environment where
# no memory is allocated, and then passing data to the worker processes as needed.
# Because we can't rely on the Pool from multiprocessing, we will have to manage the workers
# ourselves.

### Utility before class definitions
### Making lambdas pickleable
# CodeType __new__ method
# def __new__(
#             cls,
#             __argcount: int,
#             __posonlyargcount: int,
#             __kwonlyargcount: int,
#             __nlocals: int,
#             __stacksize: int,
#             __flags: int,
#             __codestring: bytes,
#             __constants: tuple[object, ...],
#             __names: tuple[str, ...],
#             __varnames: tuple[str, ...],
#             __filename: str,
#             __name: str,
#             __firstlineno: int,
#             __linetable: bytes,
#             __freevars: tuple[str, ...] = ...,
#             __cellvars: tuple[str, ...] = ...,
#         ) -> Self: ...
# All of these arguments are required to create a new CodeType object
# They are all, individually, picklable
# The primary issue with pickling a lambda is recreating the code object
# Therefore, if we can pickle the code object, we can pickle the lambda
#
# LambdaType __new__ method
# def __new__(
#         cls,
#         code: CodeType,
#         globals: dict[str, Any],
#         name: str | None = ...,
#         argdefs: tuple[object, ...] | None = ...,
#         closure: tuple[_Cell, ...] | None = ...,
#     ) -> Self: ...
# The LambdaType object requires a CodeType object to be created
# Closure is not necessary, and can be ignored
def make_code_picklable(code:CodeType)->dict:
    data = {
        "argcount": code.co_argcount,
        "posonlyargcount": code.co_posonlyargcount,
        "kwonlyargcount": code.co_kwonlyargcount,
        "nlocals": code.co_nlocals,
        "stacksize": code.co_stacksize,
        "flags": code.co_flags,
        "codestring": code.co_code,
        "constants": code.co_consts,
        "names": code.co_names,
        "varnames": code.co_varnames,
        "filename": code.co_filename,
        "name": code.co_name,
        "firstlineno": code.co_firstlineno,
        "linetable": code.co_lnotab,
        "freevars": code.co_freevars,
        "cellvars": code.co_cellvars
    }
    data["type"] = "codetype"
    return data
def make_lambda_picklable(lam:Callable)->dict:
    code_obj = lam.__code__
    code_dict = make_code_picklable(code_obj)
    name = lam.__name__
    argdefs = lam.__defaults__
    data = {
        "type": "lambda",
        "code": code_dict,
        "name": name,
        "argdefs": argdefs
    }
    return data

def make_code_from_dict(data:dict)->CodeType:
    if not data.get("type") == "codetype":
        raise ValueError("Data is not a code type")
    return CodeType(
        data["argcount"],
        data["posonlyargcount"],
        data["kwonlyargcount"],
        data["nlocals"],
        data["stacksize"],
        data["flags"],
        data["codestring"],
        data["constants"],
        data["names"],
        data["varnames"],
        data["filename"],
        data["name"],
        data["firstlineno"],
        data["linetable"],
        data["freevars"],
        data["cellvars"]
    )
def make_lambda_from_dict(data:dict)->Callable:
    # if not data.get("type") == "lambda":
    #     return default_dict_dispatch(data)
    code_dict = data["code"]
    code_obj = make_code_from_dict(code_dict)
    name = data["name"]
    argdefs = data["argdefs"]
    return LambdaType(
        code_obj,
        globals(),
        name,
        argdefs
    )


## Worker Class: Smallest unit, stores information and manages a single worker process
# Initializes basic data on startup, and resets and reinitializes the worker process
# when a task is received.
# Additionally, stores state information and statistics on the worker process
class Worker:
    def __init__(self, worker_id:int, output_folder:Path):
        self.worker_id = worker_id
        self.transfer_path = output_folder / f"worker_{worker_id}.pickle"
        self.stats = {
            "timing": {
                "start": None,
                "end": None,
                "last": None,
                "last_dur": None,
                "idle": 0
            }
        }
        self.state = "idle"
        self.initialized = False
        self.process = None
        self.conn_parent, self.conn_child = Pipe()
        self.taskdata = None

    def do_timing(self):
        if self.stats["timing"]["start"] is None:
            self.stats["timing"]["start"] = time.time()
        _last = self.stats["timing"]["last"] or self.stats["timing"]["start"]
        self.stats["timing"]["last"] = time.time()
        self.stats["timing"]["last_dur"] = _last - self.stats["timing"]["last"]
        if self.state == "idle":
            self.stats["timing"]["idle"] += self.stats["timing"]["last_dur"]

    def start(self):
        self.process = Process(target=self.run, args=(self.conn_child,))
        for p in self.transfer_path.parents:
            if not p.exists():
                p.mkdir()
        self.process.start()
        self.do_timing()
        self.initialized = True

    def run(self, conn_child:Connection):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        this_process = psutil.Process()
        parent = this_process.parent()
        self.state = "running"
        while True:
            if not parent.is_running():
                break
            task = conn_child.poll(timeout=0.1)
            task = conn_child.recv() if task else None
            if task == "exit":
                break
            self.do_timing()
            self.state = "working" if task is not None else "idle"
            if task is None:
                continue
            self.process_task(task, conn_child)
            # print(f"{self.worker_id}: Task completed")
            conn_child.send("done")
        self.state = "exiting"
        conn_child.close()
        this_process.terminate()

    def process_task(self, task, conn_child:Connection):
        if isinstance(task, str):
            task = json.loads(task)
        elif isinstance(task, bytes):
            task = pickle.loads(task)
        func = task["func"]
        if isinstance(func, dict):
            func = make_lambda_from_dict(func)
        args = task["args"] if "args" in task else task["data"]
        if "filepath" in task:
            with open(task["filepath"], "rb") as f:
                data = pickle.load(f)
            _ret = func(data, *args)
        else:
            _ret = func(*args)
        if "ret_path" in task:
            with open(task["ret_path"], "wb") as f:
                pickle.dump(_ret, f)
        else:
            conn_child.send(pickle.dumps(_ret))

    def send_task(self, args:dict):
        #Ensure we never send more than 32MiB of data over the pipe
        _data = pickle.dumps(args["data"])
        if isinstance(args["func"], dict):
            args["func"] = make_lambda_from_dict(args["func"])
        if isinstance(args["func"], Callable) and "lambda" in args["func"].__name__: #Lambda functions are not pickleable
            args["func"] = make_lambda_picklable(args["func"])
        self.taskdata = {}
        if sys.getsizeof(_data) > 4 * 1024 * 1024:
            args["data"] = None
            args["filepath"] = self.transfer_path
            args["ret_path"] = self.transfer_path
            with open(self.transfer_path, "wb") as f:
                pickle.dump(_data, f)
            self.taskdata["filepath"] = True
        try:
            self.conn_parent.send(pickle.dumps(args))
        except Exception as e:
            print(f"Error sending task: {e}")
            print(f"Task data: {args}")
            self.stop()
            raise e

    def get_return(self):
        while not self.conn_parent.poll():
            if not self.process.is_alive():
                self.stop()
                raise Exception("Process has died")
        if self.taskdata.get("filepath"):
            with open(self.transfer_path, "rb") as f:
                return pickle.load(f)
        rets = pickle.loads(self.conn_parent.recv())
        done = self.conn_parent.recv()
        if done != "done":
            print(f"Error in getting return: {done}")
            self.stop()
            raise Exception("Error in getting return")
        return rets
    
    def reset(self):
        self.state = "idle"
        self.stats["timing"]["start"] = None
        self.stats["timing"]["end"] = None
        self.stats["timing"]["last"] = None
        self.stats["timing"]["last_dur"] = None
        self.stats["timing"]["idle"] = 0
        self.taskdata = None

    def stop(self):
        self.conn_parent.send("exit")
        self.process.join()
        self.process.close()
        self.process = None
        self.initialized = False
        self.reset()

    def check_alive(self):
        if self.process is None:
            return False
        return self.process.is_alive()
    
    def poll(self):
        if self.process is None:
            return False
        if self.taskdata is None:
            return False
        return self.conn_parent.poll()
    
    def terminate(self):
        if self.process is not None:
            self.process.terminate()
            self.process.join()
            self.process.close()
            self.process = None
            self.initialized = False
            self.reset()

test_worker = False
if __name__ == "__main__" and test_worker:
    output_folder = os.path.join(os.path.dirname(__file__), "output")
    worker = Worker(1, Path(output_folder))
    worker.start()
    worker.send_task({
        "func": sum,
        "data": [[i for i in range(10000)]]
    })
    print(worker.get_return())

    for i in range(10):
        worker.send_task({
            "func": sum,
            "data": [[i for i in range(10000)]]
        })
        print(worker.get_return())
    worker.stop()
    print("Worker stopped")

## WorkerManager Class: Manages a set of workers, and distributes tasks to them
# The WorkerManager class is responsible for managing a set of workers.
# It initializes the workers, and distributes tasks to them.
# The WorkerManager can be given a group of tasks, and will distribute them to the workers
# as they become available.
class WorkerManager:
    _instances = {}
    def __new__(cls, uuid:str, *args, **kwargs):
        if uuid not in cls._instances:
            cls._instances[uuid] = super().__new__(cls)
        return cls._instances[uuid]
    
    def __init__(self, uuid:str, output_folder:Path, num_workers:int=cpu_count()):
        self.uuid = uuid
        self.output_folder = output_folder
        self.num_workers = num_workers
        self.workers = [Worker(i, output_folder) for i in range(num_workers)]
        self.task_queue = Queue()
        self.task_count = 0
        self.task_data = {}
        self.task_stats = {}
        self.task_return = {}
        self.worker_tasks = {}
        self.tasks_sent = 0
        self.tasks_done = 0
        atexit.register(self.terminate)

    def start(self):
        for worker in self.workers:
            worker.start()

    def send_task(self, func:Callable, *args, data=None, **kwargs):
        if "<lambda>" in func.__name__: #Lambda functions are not pickleable
            func = make_lambda_picklable(func)
        self.task_queue.put({
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "data": data
        })
        self.task_count += 1

    def send_task_chunk(self, func:Callable, dataset:list, chunk_size:int=1000):
        for i in range(0, len(dataset), chunk_size):
            self.send_task(func, dataset[i:i+chunk_size])

    def get_status(self):
        data = {
            "task_count": self.task_count,
            "task_data": self.task_data,
            "task_stats": self.task_stats,
            "worker_status": {}
        }
        for i, worker in enumerate(self.workers):
            data["worker_status"][i] = {
                "state": worker.state,
                "stats": worker.stats
            }
        return data
    
    def distribute(self):
        try:
            _i = 0
            last_done = 0
            while self.task_count > self.tasks_done:
                for worker in self.workers:
                    if worker.state == "idle" and not self.task_queue.empty() and worker.taskdata is None:
                        task = self.task_queue.get()
                        worker.send_task(task)
                        self.task_data[self.tasks_sent] = (worker.worker_id,)
                        self.worker_tasks[worker.worker_id] = self.tasks_sent
                        self.task_stats[worker.worker_id] = {
                            "start": time.time()
                        }
                        self.tasks_sent += 1
                        # break
                for worker in self.workers:
                    if worker.poll():
                        ret = worker.get_return()
                        task_id = self.worker_tasks[worker.worker_id]
                        self.task_return[task_id] = ret
                        self.task_stats[worker.worker_id]["end"] = time.time()
                        self.tasks_done += 1
                        worker.reset()
                if self.tasks_done % 10 == 0 and self.tasks_done > last_done:
                    print(f"Tasks done: {self.tasks_done}/{self.task_count}")
                    last_done = self.tasks_done
                time.sleep(0.1)
                _i += 1
                # if _i%100 == 0 or _i == 50:
                #     print(f"Tasks done: {self.tasks_done}/{self.task_count}")
                #     status = self.get_status()
                #     for k, v in status["worker_status"].items():
                #         print(f"Worker {k}: {v['state']}")
                #     print(f"Task return: {len(self.task_return)}/{self.task_count}")
                #     for k, v in self.task_data.items():
                #         print(f"Task {k}: Taken by worker {v[0]}, status {status['worker_status'][v[0]]['state']}")
            print(f"Tasks done: {self.tasks_done}/{self.task_count}")
        except Exception as e:
            print(f"Error in distributing tasks: {e}")
            self.terminate()
            raise e
    
    def stop(self):
        for worker in self.workers:
            worker.stop()
        self.task_queue.close()

    def terminate(self):
        print("Terminating Worker Manager")
        for i, worker in enumerate(self.workers):
            if worker.check_alive():
                worker.terminate()
            print(i, end=" ")
        print()
        self.task_queue.close()

    def reset(self):
        for worker in self.workers:
            worker.reset()
        self.task_count = 0
        self.task_data = {}
        self.task_stats = {}
        self.task_return = {}
        self.worker_tasks = {}
        self.tasks_sent = 0
        self.tasks_done = 0

test_worker_manager = True
if __name__ == "__main__" and test_worker_manager:
    output_folder = os.path.join(os.path.dirname(__file__), "output")
    wm = WorkerManager("test", Path(output_folder))
    wm.start()
    for i in range(50):
        wm.send_task(sum, [random.randint(0, 1000) for i in range(10000)])
    wm.distribute()
    print(wm.task_return)
    wm.reset()
    string_join = lambda x: "".join(x)
    for i in range(50):
        wm.send_task(string_join, [str(random.randint(0, 1000)) for i in range(10000)])
    wm.distribute()
    print([len(v) for k, v in wm.task_return.items()])
    wm.stop()
    print("Worker Manager stopped")



            
            
            
    



    









    