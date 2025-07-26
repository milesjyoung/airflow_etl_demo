from typing import List, Dict
import os
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, conint
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
import uuid

os.environ["OPENAI_API_KEY"] = "your-key" # dont use in production (for GCP use secret manager and its automatically picked up)

# Define your schema
class SolarSystem(BaseModel):
    system_size: float | None = Field(None, description="Size of system in kW DC")
    permit_type: str | None = Field(None, enum=["Roof Mount", "Ground Mount", "Storage only"])
    storage_size: float | None = Field(None, description="Storage size in kWh")
    module_watts: conint(ge=200, le=500) | None = Field(None, description="Watts per module")
    module_count: int | None = Field(None, description="Number of modules")
    module_brand: str | None = Field(None, description="Brand of modules")

# Prompt with examples
prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are an expert extraction algorithm. "
            "Only extract relevant information from the text. "
            "If you do not know the value of an attribute asked "
            "to extract, return null for the attribute's value.",
        ),
        # ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
        MessagesPlaceholder("examples"),  # <-- EXAMPLES!
        # ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
        ("human", "{text}"),
    ]
)

def tool_example_to_messages(example: Dict) -> List:
    messages = [HumanMessage(content=example["input"])]
    tool_calls = [
        {
            "id": str(uuid.uuid4()),
            "args": tool.dict(),
            "name": tool.__class__.__name__,
        }
        for tool in example["tool_calls"]
    ]
    messages.append(AIMessage(content="", tool_calls=tool_calls))
    for output, call in zip(example.get("tool_outputs") or ["You have correctly called this tool."] * len(tool_calls), tool_calls):
        messages.append(ToolMessage(content=output, tool_call_id=call["id"]))
    return messages

# Define example messages
examples_raw = [
    (
        "Digital.  19 - REC NPeak2 365 Watt Modules.  1 - SolarEdge HD-Wave 7600H Inverter.  Signed SCMA Affidavit on file.  Includes two inspections.  Final required.",
        SolarSystem(system_size=None, permit_type=None, storage_size=None, module_watts=365, module_count=19, module_brand="REC NPeak2"),
    ),
    (
        "Web.  Roof PV solar on storage building - 9.36kW - 26 panels - 2 batteries (13.5 kWh each). 100' trench. Includes two inspections.  Final required.",
        SolarSystem(system_size=9.36, permit_type="Roof Mount", storage_size=27, module_watts=None, module_count=26, module_brand=None),
    ),
    (
        "Digital: 5.53 Kw - 14 Modules PV roof top solar mount and (1) 13.5Kw Battery to be installed on detached garage.This permit includes two inspections",
        SolarSystem(system_size=5.53, permit_type="Roof Mount", storage_size=13.5, module_watts=None, module_count=14, module_brand=None),
    ),
]
example_messages = []
for text, tool_call in examples_raw:
    example_messages.extend(tool_example_to_messages({"input": text, "tool_calls": [tool_call]}))

# Inference pipeline
llm = ChatOpenAI(model="gpt-4-0125-preview", temperature=0)
runnable = prompt | llm.with_structured_output(
    schema=SolarSystem,
    method="function_calling",
    include_raw=False,
)

def extract_details_batch(descriptions: List[str]) -> List[Dict]:
    inputs = [{"text": desc, "examples": example_messages} for desc in descriptions]
    results = runnable.batch(inputs)
    return [r.dict() for r in results]