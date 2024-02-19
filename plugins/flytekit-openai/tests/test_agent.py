from datetime import timedelta
from unittest import mock

import pytest

from flytekit import FlyteContext
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


async def mock_acreate(*args, **kwargs):
    mock_response = mock.MagicMock()
    mock_choice = mock.MagicMock()
    mock_choice.message.content = "mocked_message"
    mock_response.choices = [mock_choice]
    return mock_response


@pytest.mark.asyncio
async def test_chatgpt_agent():
    agent = AgentRegistry.get_agent("chatgpt")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_config = {
        "openai_organization": "test-openai-orgnization-id",
        "chatgpt_config": {"model": "gpt-3.5-turbo", "temperature": 0.7},
    }
    task_metadata = TaskMetadata(
        True,
        RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    tmp = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=None,
        type="chatgpt",
    )

    task_inputs = literals.LiteralMap(
        {
            "message": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="Test ChatGPT Plugin"))
            ),
        },
    )
    output_prefix = FlyteContext.current_context().file_access.get_random_local_directory()
    # import openai.types.chat.chat_completion.ChatCompletion

    # # Mock the AsyncOpenAI constructor
    # with mock.patch('openai._client.AsyncOpenAI', autospec=True) as mock_openai:
    #     # Setup the mock instance
    #     mock_instance = mock_openai.return_value
    #     mock_instance.chat.completions.create = mock.MagicMock(return_value=asyncio.Future())
    #     mock_instance.chat.completions.create.return_value.set_result(mock_acreate())

    #     # Mock get_agent_secret to return a fake secret
    #     with mock.patch("flytekit.extend.backend.base_agent.get_agent_secret", return_value="mocked_secret"):
    #         # Now, when agent.create() is called, it will use the mocked AsyncOpenAI instance
    #         response = await agent.create(output_prefix, tmp, task_inputs)

    # # with mock.patch("openai.AsyncOpenAI.chat.completions.create", new=mock_acreate):
    # #     with mock.patch("flytekit.extend.backend.base_agent.get_agent_secret", return_value="mocked_secret"):
    # #         # Directly await the coroutine without using asyncio.run
    # #         response = await agent.create(output_prefix, tmp, task_inputs)

    # assert response.HasField("resource")
    # assert response.resource.phase == TaskExecution.SUCCEEDED
    # assert response.resource.outputs is not None
    # print(response.resource.outputs)
