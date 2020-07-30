from parsl import python_app
import logging
from concurrent.futures import Future

logger = logging.getLogger(__name__)


@python_app(executors=['submit-node'])
def map_over_future(func, future):
    return func(future)


@python_app(executors=['submit-node'])
def combine(inputs=[]):
    # do nothing, but not until the inputs are all ready: joins multiple futures into a single future that completes when all inputs are completed.
    # this doesn't need a full parsl task, but it can be implemented as one so I'm staying in the parsl domain for now. Staying in the parsl domain means that parsl can see this as a task, and so show dependencies in monitoring.
    return


def const_future(val):
    f = Future()
    f.set_result(val)
    return f
