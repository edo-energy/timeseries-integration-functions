import datetime
import logging

import azure.functions as func
import azure.durable_functions as df

integrations = [
    "WeatherCollection"
]


async def main(dailyTimer: func.TimerRequest, starter: str) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info(f"Starting to spawn daily integrations at {utc_timestamp}")

    client = df.DurableOrchestrationClient(starter)

    for integration in integrations:
        logging.info(f"Sending due integration {integration}")

        instance_id = await client.start_new(integration)

        logging.info(f"Started integration with ID '{instance_id}'.")
