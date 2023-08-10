import json
from dataclasses import asdict, dataclass
from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)

# for databricks
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from flytekit import FlyteContextManager, StructuredDataset, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentBase,
    AgentRegistry,
)
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType


@dataclass
class Metadata:
    # cluster_id: str
    # host: str
    # token: str
    run_id: int


class DatabricksAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="spark")
        print("@@@ DatabricksAgent.__init__")

    def create(
        self,
        context: grpc.ServicerContext,  # from bigquery/agent.py, not sure
        output_prefix: str,  # from bigquery/agent.py, not sure
        task_template: TaskTemplate,  # from plugin.go
        inputs: Optional[LiteralMap] = None,  # from bigquery/agent.py, not sure
    ) -> CreateTaskResponse:
        print("@@@ DatabricksAgent->create (method)")
        custom = task_template.custom
        tasks = [
            jobs.Task(
                description=custom["Description"],  # metadata
                existing_cluster_id=custom["ClusterID"],  # metadata
                spark_python_task=jobs.SparkPythonTask(python_file=custom["PythonFile"]),  # metadata
                task_key=custom["TaskKey"],  # metadata
                timeout_seconds=custom["TimeoutSeconds"],  # metadata
            )
        ]

        w = WorkspaceClient()

        run = w.jobs.submit(
            name=f"sdk-demo-python-task",  # metadata
            tasks=tasks,  # tasks
        ).result()

        # metadata
        metadata = Metadata(
            # cluster_id=task_template.cluster_id,
            # host=task_template.host,
            # token=task_template.token,
            run_id=run.tasks[0].run_id,
        )

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        print("@@@ DatabricksAgent->get (method)")
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        w = WorkspaceClient()
        job = w.jobs.get_run_output(metadata.run_id)

        if job.error:  # have already checked databricks sdk
            logger.error(job.errors.__str__())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(job.errors.__str__())
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))

        if job.metadata.state.result_state == jobs.RunResultState.SUCCESS:
            cur_state = SUCCEEDED
        else:
            # TODO: Discuss with Kevin for the state, considering mapping technique
            cur_state = PERMANENT_FAILURE

        res = None

        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            # output page_url and task_output
            if job.metadata.run_page_url:
                output_location = job.metadata.run_page_url
                res = literals.LiteralMap(
                    {
                        "results": TypeEngine.to_literal(
                            ctx,
                            StructuredDataset(uri=output_location),
                            StructuredDataset,
                            LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                        )
                    }
                ).to_flyte_idl()

        return GetTaskResponse(resource=Resource(state=cur_state, outputs=res))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        print("@@@ DatabricksAgent->delete (method)")
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        w = WorkspaceClient()
        w.jobs.delete_run(metadata.run_id)
        return DeleteTaskResponse()


AgentRegistry.register(DatabricksAgent())
