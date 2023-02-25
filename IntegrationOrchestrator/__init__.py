import logging

import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Starting integration orchestration.")
    (activity_name, input_params) = context.get_input()

    logging.info(f"Running integration for {activity_name}.")
    result = yield context.call_activity(activity_name, input_params)

    logging.info("Integration orchestration completed.")
    return result


main = df.Orchestrator.create(orchestrator_function)
