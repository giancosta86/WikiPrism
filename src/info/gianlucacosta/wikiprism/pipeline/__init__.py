from typing import TypeVar

from info.gianlucacosta.eos.core.threading.cancelable import CancelableThreadHandle

from .strategy import PipelineStrategy
from .supervisor import SupervisorThread

TTerm = TypeVar("TTerm")

PipelineHandle = CancelableThreadHandle


def run_extraction_pipeline(
    strategy: PipelineStrategy[TTerm],
) -> PipelineHandle:
    """
    Runs a wiki extraction pipeline that reads a source wiki file and creates
    a custom dictionary of terms.

    In order to customize the implementation details, you'll need to pass an instance
    of a PipelineStrategy subclass.
    """

    supervisor_thread = SupervisorThread(strategy)
    supervisor_thread.start()
    return CancelableThreadHandle(supervisor_thread)
