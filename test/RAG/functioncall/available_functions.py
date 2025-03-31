import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
 
from function_descriptions import TABLE_CATEGORY
from agent.agent_function import query_agent

from functools import partial

all_functions = TABLE_CATEGORY


def update_available_functions():
    functions = all_functions
    available_functions = {}
    for function in functions:
        function_name = function["function"]["name"]

        if function_name == "client_subscription":
            available_functions[function_name] = partial(
                query_agent, table_name="client_subscription.json"
            )

        elif function_name == "client_support":
            available_functions[function_name] = partial(
                query_agent, table_name="client_support.json"
            )

        elif function_name == "client_onboarding":
            available_functions[function_name] = partial(
                query_agent, table_name="client_onboarding.json"
            )

    return available_functions
