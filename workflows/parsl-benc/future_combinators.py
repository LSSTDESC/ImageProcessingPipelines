from concurrent.futures import Future
from parsl import python_app
from parsl.process_loggers import wrap_with_logs
import logging

logger = logging.getLogger(__name__)


@python_app(executors=['submit-node'])
def map_over_future(func, future):
    return func(future)


@python_app(executors=['submit-node'])
def combine(inputs=[]):
    # do nothing, but not until the inputs are all ready: joins multiple futures into a single future that completes when all inputs are completed.
    # this doesn't need a full parsl task, but it can be implemented as one so I'm staying in the parsl domain for now. Staying in the parsl domain means that parsl can see this as a task, and so show dependencies in monitoring.
    return


class JoinFuture(Future):
    """A JoinFuture takes a future which will itself return a future.
    The JoinFuture will complete when the inner future completes.
    The inner future does not need to be known at the time that the
    JoinFuture is initialized, because it is supplied by the outer
    future at a later time."""
    def __init__(self, outer_future):
        super().__init__()
        logger.info("Creating a JoinFuture")
        outer_future.add_done_callback(self._outer_done)

    @wrap_with_logs
    def _outer_done(self, outer_future):
        logger.info("JoinFuture: _outer_done")
        if outer_future.exception():
            self.set_exception(outer_future.exception())
        else:
            inner_future = outer_future.result()
            inner_future.add_done_callback(self._inner_done)

    @wrap_with_logs
    def _inner_done(self, inner_future):
        logger.info("JoinFuture: _inner_done")
        if inner_future.exception():
            self.set_exception(inner_future.exception())
        else:
            self.set_result(inner_future.result())
